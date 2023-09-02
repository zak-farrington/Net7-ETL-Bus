using Azure.Messaging.ServiceBus;
using CsvHelper;
using EFCore.BulkExtensions;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Net7EtlBus.Models;
using Net7EtlBus.Service.Data;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Models.EtlBusDb;
using Npgsql.EntityFrameworkCore.PostgreSQL;
using System.Formats.Asn1;
using System.Globalization;
using System.Threading.Tasks.Dataflow;

namespace Net7EtlBus.Service
{
    public class ServiceBusWorker : BackgroundService
    {
        private readonly ILogger<ServiceBusWorker> _logger;
        private readonly IConfiguration _appConfig;
        private readonly IGoogleApiService _googleApiService;

        private readonly string _sbConnectionString;
        private readonly string _sbQueueName;
        private readonly Lazy<ServiceBusClient> _serviceBusClientLazy;

        public ServiceBusWorker(ILogger<ServiceBusWorker> logger, IConfiguration appConfig, IGoogleApiService googleApiService)
        {
            _logger = logger;
            _appConfig = appConfig;
            _googleApiService = googleApiService;

            _sbConnectionString = _appConfig["ServiceBus.ConnectionPrimary"] ?? throw new InvalidOperationException("ServiceBusConnectionPrimary API key is missing."); ;
            _sbQueueName = _appConfig["ServiceBus.QueueName"] ?? throw new InvalidOperationException("ServiceBusQueue name is is missing."); ;
            _serviceBusClientLazy = new Lazy<ServiceBusClient>(() => new ServiceBusClient(_sbConnectionString));
        }

        /// <summary>
        /// Entry point for BackgroundService.
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // TODO: Handle auto queue creation if neccessary
            // await CreateQueueIfNotExists(_sbQueueName);
            _logger.LogInformation("Background worker started.");

            var serviceBusProcessor = _serviceBusClientLazy.Value.CreateProcessor(_sbQueueName, new ServiceBusProcessorOptions());

            // Configure the message and error handlers
            serviceBusProcessor.ProcessMessageAsync += RunAsync;
            serviceBusProcessor.ProcessErrorAsync += ErrorHandler;

            //Start processing
            await serviceBusProcessor.StartProcessingAsync(stoppingToken);
            _logger.LogInformation("Service Bus listener has been started.");

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        // TODO: Handle auto queue creation if neccessary
        //private async Task CreateQueueIfNotExists(string queueName)
        //{
        //    var serviceBusManagementClient = new ServiceBusManagementClient();
        //    if (!(await serviceBusManagementClient.QueueExistsAsync(queueName)))
        //    {
        //        await serviceBusManagementClient.CreateQueueAsync(queueName);
        //    }
        //}

        /// <summary>
        /// Event listener for service bus messages.
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private async Task RunAsync(ProcessMessageEventArgs args)
        {
            try
            {
                _logger.LogInformation("Message has been receieved.");

                // Step 1 - check if there is a file for processing
                // TODO: Download from FTP and extract 
                var csvFileName = "geo_data.csv";
                
                var etlRunConditions = await EvaluateEtlRunConditionsAsync(csvFileName).ConfigureAwait(false);
                if (!etlRunConditions.ShouldRun)
                {
                    _logger.LogError("Unable to continue. This run will abort.");
                    await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
                    return;
                }

                // Record must be updated once we have finished processing.
                var csvImportRecord = etlRunConditions.EtlBusImport;

                // Step 2 - read records from CSV
                var zipCodeRecordsHashMap = GetZipCodesFromCsv(csvFileName);
                var hasRecords = zipCodeRecordsHashMap.Any();

                if (!hasRecords)
                {
                    // Error encountered or no mappings to process.
                    _logger.LogError("Unable to continue. No mappings parsed from CSV. This run will abort.");
                    await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
                    return;
                }
                else
                {
                    _logger.LogInformation($"Found ${zipCodeRecordsHashMap.Count} records in {csvFileName}");

                    // TODO: We have zip codes, perform TPL
                    // Step 3 - TPL data flow to update and save records
                    var bufferBlock = new BufferBlock<ZipCodeDetails>();

                    var transformBlock = new TransformBlock<ZipCodeDetails, ZipCodeDetails>(async zipCodeRecord =>
                    {
                        var latLng = await _googleApiService.GetLatLngFromZipAsync(zipCodeRecord.ZipCode).ConfigureAwait(false);

                        if (latLng.IsSuccessful)
                        {
                            zipCodeRecord.Latitude = latLng.Latitude.Value;
                            zipCodeRecord.Longitude = latLng.Longitude.Value;

                            var latLngString = $"{latLng.Latitude},{latLng.Longitude}";

                            var elevationResponse = await _googleApiService.GetElevationAsync(latLngString).ConfigureAwait(false);
                            if (elevationResponse.IsSuccessful)
                            {
                                zipCodeRecord.Elevation = elevationResponse.Elevation.Value;
                            }

                            var timeZoneResponse = await _googleApiService.GetTimeZoneAsync(latLngString).ConfigureAwait(false);
                            if (timeZoneResponse.IsSuccessful)
                            {
                                zipCodeRecord.Timezone = timeZoneResponse.TimeZoneName;
                            }
                        }
                        return zipCodeRecord;
                    }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 5 });  // Adjust as per Google API rate limits

                    using var throttler = new SemaphoreSlim(1);

                    var batchedZipCodeDetailsForUpserting = new List<ZipCodeDetails>();
                    var actionBlock = new ActionBlock<ZipCodeDetails>(async zipCodeRecord =>
                    {
                        zipCodeRecord.LastModifiedDateUtc = DateTime.UtcNow;

                        try
                        {
                            await throttler.WaitAsync().ConfigureAwait(false);
                            batchedZipCodeDetailsForUpserting.Add(zipCodeRecord);

                            // Bulk save up to 25 records at a time
                            if (batchedZipCodeDetailsForUpserting.Count >= 5)
                            {
                                // Use Npgsql library for Upsert
                                await using var etlBusDbContext = new EtlBusDbContext(_appConfig);
                                await etlBusDbContext.BulkInsertOrUpdateAsync(batchedZipCodeDetailsForUpserting).ConfigureAwait(false);
                                await etlBusDbContext.SaveChangesAsync().ConfigureAwait(false);

                                // Reset batch
                                batchedZipCodeDetailsForUpserting.Clear();
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "There was a problem saving changes to the database.");
                        }
                        finally
                        {
                            throttler.Release();

                        }
                    }, new ExecutionDataflowBlockOptions { BoundedCapacity = 1 });
                
                    bufferBlock.LinkTo(transformBlock, new DataflowLinkOptions { PropagateCompletion = true });
                    transformBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

                    // TODO: Only process zip codes that are missing details, so check db for any existing records.

                    // Load zip codes into buffer block
                    foreach (var zipCodeRecord in zipCodeRecordsHashMap.Values.Take(10))  // TODO: Remove take when debugging.
                    {
                        var zipCodeWithDetails = new ZipCodeDetails
                        {
                            ZipCode = zipCodeRecord.ZipCode,
                            StateCode = zipCodeRecord.StateCode,
                            State = zipCodeRecord.State,
                            County = zipCodeRecord.County,
                            City = zipCodeRecord.City,
                        };
                        bufferBlock.Post(zipCodeWithDetails);
                    }
                    
                    // Step 4 - complete, delete file, update import record
                    bufferBlock.Complete();
                    await actionBlock.Completion.ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Unhandled error has been countered while processing message.");
                throw ex;
            }
            finally
            {
                _logger.LogInformation("Message has been completed.");
                await args.CompleteMessageAsync(args.Message).ConfigureAwait(false); ;
            }
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "Service bus error handler encountered exception.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Determine if ETL process already ran or should continue.
        /// </summary>
        /// <returns></returns>
        private async Task<EtlRunConditions> EvaluateEtlRunConditionsAsync(string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                // No file found
                _logger.LogInformation("No CSV file available. Will not process.");
                return new EtlRunConditions
                {
                    ShouldRun = false,
                    FileNameForProcessing = fileName,
                };
            }

            string fileChecksum = Utilities.FileSystem.GetFileChecksum(fileName);

            var etlBusImport = new EtlBusImport
            {
                ImportStartTimeUtc = DateTime.UtcNow,
                IsActive = true,
                FileName = fileName,
                FileChecksum = fileChecksum,
            };

            //using var dbContext = new EtlBusDbContext();
            ////var hasAlreadyProcessed = dbContext.EtlBusImport.Where();

            //if (false)
            //{
            //    _logger.LogInformation("CSV file has already been processed. Will not continue.");
            //    Utilities.FileSystem.DeleteFile(fileName);
            //    return new EtlRunConditions
            //    {
            //        ShouldRun = false,
            //        FileNameForProcessing = fileName,
            //    };
            //}
            //await dbContext.AddAsync(csvImportRecord).ConfigureAwait(false);
            //await dbContext.SaveChangesAsync().ConfigureAwait(false);

            return new EtlRunConditions
            {
                ShouldRun = true,
                FileNameForProcessing = fileName,
                EtlBusImport = etlBusImport,
            };
        }

        /// <summary>
        /// Read records from CSV file.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filePath"></param>
        /// <returns></returns>
        private static IEnumerable<T> GetRecordsFromCsv<T>(string filePath)
        {
            using var reader = new StreamReader(filePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<T>().ToList();

            return records;
        }

        /// <summary>
        /// Parse CSV file and return mappings along with duplicates.
        /// </summary>
        /// <param name="fileName">Full file path to CSV file.</param>
        /// <returns>Object that contains parsed mappings.</returns>
        private Dictionary<string, ZipCodeRecord> GetZipCodesFromCsv(string fileName)
        {
           var zipCodesHashMap = new Dictionary<string, ZipCodeRecord>();

            try
            {
                foreach (var record in GetRecordsFromCsv<ZipCodeRecord>(fileName).ToList())
                {
                    var zipCode = record.ZipCode;

                    if (!zipCodesHashMap.ContainsKey(zipCode))
                    {
                        // no duplicates, add to dictionary
                        zipCodesHashMap.Add(zipCode, record);
                    }
                    // TODO: handle duplicates.
                }

            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, $"There was a problem reading CSV {fileName}");
            }

            return zipCodesHashMap;
        }
    }
}
