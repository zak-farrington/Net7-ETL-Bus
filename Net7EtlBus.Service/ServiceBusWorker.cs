using Azure.Messaging.ServiceBus;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Net7EtlBus.Models;
using Net7EtlBus.Service.Core.Concretes;
using Net7EtlBus.Service.Core.Interfaces;
using Net7EtlBus.Service.Data;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Models.EtlBusDb;
using Net7EtlBus.Service.Utilities;
using Newtonsoft.Json;
using System.Collections.Concurrent;
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
        private readonly ServiceBusClient _serviceBusClient;
        private readonly Lazy<IDataHandler> _dataHandlerLazy;

        private readonly int _validRecordDaysTtl;
        private readonly int _transformMaxDegreeOfParallelism;
        private readonly int _actionBoundedCapactiy;
        private readonly int _batchRecordSaveCount;

        public ServiceBusWorker(ILogger<ServiceBusWorker> logger, IConfiguration appConfig, IGoogleApiService googleApiService)
        {
            _logger = logger;
            _appConfig = appConfig;
            _googleApiService = googleApiService;
            _dataHandlerLazy = new Lazy<IDataHandler>(() => new CsvDataHandler());

            _sbConnectionString = _appConfig["ServiceBus.ConnectionPrimary"] ?? throw new InvalidOperationException("ServiceBusConnectionPrimary API key is missing.");
            _sbQueueName = _appConfig["ServiceBus.QueueName"] ?? throw new InvalidOperationException("ServiceBusQueue name is is missing.");

            _validRecordDaysTtl = int.TryParse(_appConfig["ProcessingSettings.ValidRecordDaysTtl"], out var validRecord) ? validRecord : 30;
            _transformMaxDegreeOfParallelism = int.TryParse(_appConfig["ProcessingSettings.TransformMaxDegreeOfParallelism"], out var parallelism) ? parallelism : 5;
            _actionBoundedCapactiy = int.TryParse(_appConfig["ProcessingSettings.ActionBoundedCapactiy"], out var capacity) ? capacity : 1;
            _batchRecordSaveCount = int.TryParse(_appConfig["ProcessingSettings.BatchRecordSaveCount"], out var batchCount) ? batchCount : 25;

            _serviceBusClient = new ServiceBusClient(_sbConnectionString);
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

            var serviceBusProcessor = _serviceBusClient.CreateProcessor(_sbQueueName, new ServiceBusProcessorOptions());

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
                var etlServiceBusMessage = JsonConvert.DeserializeObject<EtlServiceBusMessage>(args.Message.Body.ToString());

                _logger.LogInformation($"Message has been receieved. Force run is: {etlServiceBusMessage.ForceRun.ToString()}");

                // Step 1 - check if there is a file for processing
                // TODO: Download from FTP and extract 
                var csvFileName = "geo_data.csv";
                
                var etlRunConditions = await EvaluateEtlRunConditionsAsync(csvFileName, etlServiceBusMessage.ForceRun).ConfigureAwait(false);
                if (!etlRunConditions.ShouldRun)
                {
                    _logger.LogError("Unable to continue. This run will abort.");
                    return;
                }

                // Record must be updated once we have finished processing.
                var etlBusImportRecord = etlRunConditions.EtlBusImport;

                // Step 2 - read records from CSV
                var zipCodesPendingProcessingHashMap = _dataHandlerLazy.Value.GetRecords<ZipCodeRecord>(csvFileName)?.ToDictionary(zr => (zr.ZipCode, zr.StateCode), zr => zr);

                var recordsForProcessing = zipCodesPendingProcessingHashMap?.Keys.Count;
                var hasRecordsForProcessing = recordsForProcessing > 0;

                if (!hasRecordsForProcessing)
                {
                    // Error encountered or no mappings to process.
                    _logger.LogError("Unable to continue. No mappings parsed from CSV. This run will abort.");
                    return;
                }
                else
                {
                    _logger.LogInformation($"Found ${recordsForProcessing} records in {csvFileName}");

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
                    }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = _transformMaxDegreeOfParallelism }); 

                    using var throttler = new SemaphoreSlim(1);
                    // Storing batched records in concurrent bag to support multiple threads, if neccessary. 
                    var batchedZipCodeDetailsForUpserting = new ConcurrentBag<ZipCodeDetails>();  
                    var actionBlock = new ActionBlock<ZipCodeDetails>(async zipCodeRecord =>
                    {
                        zipCodeRecord.LastModifiedDateUtc = DateTime.UtcNow;

                        try
                        {
                            await throttler.WaitAsync().ConfigureAwait(false);
                            batchedZipCodeDetailsForUpserting.Add(zipCodeRecord);

                            // Bulk save up records
                            if (batchedZipCodeDetailsForUpserting.Count >= _batchRecordSaveCount)
                            {
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
                    }, new ExecutionDataflowBlockOptions { BoundedCapacity = _actionBoundedCapactiy });

                    bufferBlock.LinkTo(transformBlock, new DataflowLinkOptions { PropagateCompletion = true });
                    transformBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

                    // Only process zip codes that are missing details, or have not been updated recently.
                    using var etlDbContext = new EtlBusDbContext(_appConfig);

                    DateTime validRecordsThreshold = DateTime.UtcNow.AddDays((_validRecordDaysTtl * -1));  

                    // Because zip codes can cross state borders we have to use tuple & custom composite key. 
                    var existingKeys = new HashSet<string>(
                        zipCodesPendingProcessingHashMap.Keys.Select(k => ZipCodeHelpers.GetCompositeKey(k.ZipCode, k.StateCode))
                    );

                    var dbExistingValidZipCodes = etlDbContext.ZipCodeDetails
                        .Where(r =>
                            !string.IsNullOrEmpty(r.ZipCode) &&
                            existingKeys.Contains(r.CompositeKey) && 
                            r.Longitude != null &&
                            r.Latitude != null &&
                            r.Elevation != null &&
                            r.Timezone != null &&
                            r.LastModifiedDateUtc <= validRecordsThreshold
                        ).Select(r => ZipCodeHelpers.GetCompositeKey(r.ZipCode, r.StateCode))
                        .ToHashSet();

                    if (dbExistingValidZipCodes.Any())
                    {
                        _logger.LogInformation($"{dbExistingValidZipCodes.Count} exist in database will not be processed.");
                        zipCodesPendingProcessingHashMap = zipCodesPendingProcessingHashMap
                            .Where(r => !dbExistingValidZipCodes.Contains(ZipCodeHelpers.GetCompositeKey(r.Key.ZipCode, r.Key.StateCode)))
                            .ToDictionary(r => r.Key, r => r.Value);
                    }

                    if (zipCodesPendingProcessingHashMap?.Keys.Count > 0)
                    {
                        // Load zip codes into buffer block
                        foreach (var zipCodeRecord in zipCodesPendingProcessingHashMap.Values)  // TODO: Remove take when debugging.
                        {
                            var zipCodeWithDetails = new ZipCodeDetails
                            {
                                CompositeKey = ZipCodeHelpers.GetCompositeKey(zipCodeRecord.ZipCode, zipCodeRecord.StateCode),
                                ImportId = etlBusImportRecord.Id,
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

                        using var etlBusDbContext = new EtlBusDbContext(_appConfig);
                        etlBusImportRecord = await etlBusDbContext.EtlBusImports.FirstOrDefaultAsync(i => i.Id == etlBusImportRecord.Id).ConfigureAwait(false);
                        if (etlBusImportRecord != null)
                        {
                            etlBusImportRecord.Status = nameof(Constants.ProcessingStatus.Complete);
                            etlBusImportRecord.EndDateTimeUtc = DateTime.UtcNow;
                            await etlBusDbContext.SaveChangesAsync().ConfigureAwait(false);
                        }
                        else
                        {
                            _logger.LogCritical("Could not finalize import record.");
                        }
                    }

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
                await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
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
        /// <param name="fileName">File name for the CSV file to process.</param>
        /// <param name="forceRun">If true, will re-run regardless if file has already beeen processed or in progress.</param>
        /// <returns></returns>
        private async Task<EtlRunConditions> EvaluateEtlRunConditionsAsync(string fileName, bool forceRun = false)
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

            var fileChecksum = Utilities.FileSystem.GetFileChecksum(fileName);

            if(string.IsNullOrEmpty(fileChecksum)) {
                _logger.LogInformation("File checksum failed.");
                return new EtlRunConditions
                {
                    ShouldRun = false,
                    FileNameForProcessing = fileName,
                };
            }

            var etlBusImport = new EtlBusImport
            {
                ImportStartTimeUtc = DateTime.UtcNow,
                IsActive = true,
                FileName = fileName,
                FileChecksum = fileChecksum,
                Status = nameof(Constants.ProcessingStatus.Running),
            };

            using var etlBusDbContext = new EtlBusDbContext(_appConfig);
            // Check for existing record with matching checksum, where it was imported in last 30 days and has completed
            var isProcessingOrAlreadyProcessed = etlBusDbContext.EtlBusImports.Where(r => r.FileChecksum.Equals(fileChecksum) && (r.IsActive || (r.ImportStartTimeUtc > DateTime.UtcNow.AddDays(_validRecordDaysTtl * -1) && r.EndDateTimeUtc != null))).FirstOrDefault()?.Id > 0;

            if (!forceRun && isProcessingOrAlreadyProcessed)
            {
                _logger.LogInformation("CSV file is in progress or already been processed. Will not continue.");
                //Utilities.FileSystem.DeleteFile(fileName);
                return new EtlRunConditions
                {
                    ShouldRun = false,
                    FileNameForProcessing = fileName,
                };
            }
            await etlBusDbContext.AddAsync(etlBusImport).ConfigureAwait(false);
            await etlBusDbContext.SaveChangesAsync().ConfigureAwait(false);

            return new EtlRunConditions
            {
                ShouldRun = true,
                FileNameForProcessing = fileName,
                EtlBusImport = etlBusImport,
            };
        }
    }
}
