using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Identity.Client;
using Net7EtlBus.Models;
using Net7EtlBus.Service.Core.Interfaces;
using Net7EtlBus.Service.Data;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Models.EtlBusDb;
using Net7EtlBus.Service.Utilities;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using static Net7EtlBus.Service.Utilities.Constants;

namespace Net7EtlBus.Service.Core.Concretes
{

    public class DataflowProcessor : IDataflowProcessor, IDisposable
    {
        private readonly IConfiguration _appConfig;
        private readonly ILogger<DataflowProcessor> _logger;
        private readonly IGoogleApiService _googleApiService;

        private readonly ProcessingSettings _processingSettings;
        
        private EtlBusDbContext _etlBusDbContext; // Only used for tests & mocking, otherwise we instantiate on the fly for thread-safe operations.

        public DataflowProcessor(IConfiguration appConfig, ILogger<DataflowProcessor> logger, IGoogleApiService googleApiService, IOptions<ProcessingSettings> processingSettings)
            : this(appConfig, logger, googleApiService, processingSettings, null)
        {
        }

        public DataflowProcessor(IConfiguration appConfig, ILogger<DataflowProcessor> logger, IGoogleApiService googleApiService, IOptions<ProcessingSettings> processingSettings, EtlBusDbContext etlBusDbContext)
        {
            _appConfig = appConfig;
            _logger = logger;
            _googleApiService = googleApiService;
            _processingSettings = processingSettings.Value;
            _etlBusDbContext = etlBusDbContext;  // Only used for tests & mocking, otherwise we instantiate on the fly for thread-safe operations.
        }

        /// <summary>
        /// Determine if ETL process already ran or should continue.
        /// </summary>
        /// <param name="fileName">File name for the CSV file to process.</param>
        /// <param name="forceRun">If true, will re-run regardless if file has already beeen processed or in progress.</param>
        /// <returns></returns>
        public async Task<EtlRunConditions> EvaluateEtlRunConditionsAsync(string fileName, bool forceRun = false)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                // No file found.
                _logger.LogInformation("No file available. Will not process.");
                return new EtlRunConditions
                {
                    ShouldRun = false,
                    FileNameForProcessing = fileName,
                };
            }

            var fileChecksum = Utilities.FileSystem.GetFileChecksum(fileName);

            if (string.IsNullOrEmpty(fileChecksum))
            {
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


            using var etlBusDbContext = _etlBusDbContext  == null ? new EtlBusDbContext(_appConfig) : _etlBusDbContext;
            // Check for existing record with matching checksum, where it was imported since ValidRecordDaysTtl and has completed.
            var isProcessingOrAlreadyProcessed = etlBusDbContext.EtlBusImports.Where(r => r.FileChecksum.Equals(fileChecksum) && (r.IsActive || (r.ImportStartTimeUtc > DateTime.UtcNow.AddDays(_processingSettings.ValidRecordDaysTtl * -1) && r.EndDateTimeUtc != null))).FirstOrDefault()?.Id > 0;

            if (!forceRun && isProcessingOrAlreadyProcessed)
            {
                _logger.LogInformation("Import is in progress or already been processed. Will not continue.");
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

        /// <summary>
        /// Get list of records that will be processed.
        /// Excludes records that completed information and have not been updated since ValidRecordDaysTtl.
        /// </summary>
        /// <param name="zipCodesPendingProcessingHashMap"></param>
        /// <returns></returns>
        public List<ZipCodeRecord> GetRecordsExcludingPreviouslyProcessed(Dictionary<string, ZipCodeRecord>? zipCodesPendingProcessingHashMap)
        {
            using var etlBusDbContext = _etlBusDbContext == null ? new EtlBusDbContext(_appConfig) : _etlBusDbContext;

            DateTime validRecordsThreshold = DateTime.UtcNow.AddDays((_processingSettings.ValidRecordDaysTtl * -1));

            // Because zip codes can cross state borders we have to use tuple & custom composite key. 
            var existingKeys = new HashSet<string>(
                zipCodesPendingProcessingHashMap.Keys.Select(k => k)
            );

            var dbExistingValidZipCodes = etlBusDbContext.ZipCodeDetails
                .Where(r =>
                    !string.IsNullOrEmpty(r.ZipCode) &&
                    existingKeys.Contains(r.CompositeKey) &&
                    r.Longitude != null &&
                    r.Latitude != null &&
                    r.Elevation != null &&
                    r.Timezone != null &&
                    r.LastModifiedDateUtc >= validRecordsThreshold
                ).Select(r => ZipCodeHelpers.GetCompositeKey(r.ZipCode, r.StateCode))
                .ToHashSet();

            if (dbExistingValidZipCodes.Any())
            {
                _logger.LogInformation($"{dbExistingValidZipCodes.Count} records exist in database will not be processed.");
                zipCodesPendingProcessingHashMap = zipCodesPendingProcessingHashMap
                    .Where(r => !dbExistingValidZipCodes.Contains(r.Key))
                    .ToDictionary(r => r.Key, r => r.Value);
            }

            return zipCodesPendingProcessingHashMap.Values.ToList();
        }
        
        /// <summary>
        /// Setup, link TPL dataflow blocks and process records.
        /// Saves to database in batches.
        /// </summary>
        /// <param name="zipCodeRecords"></param>
        /// <param name="etlBusImportRecord"></param>
        /// <returns></returns>
        public async Task<int> InitializeAndExecuteDataflowAsync(List<ZipCodeRecord> zipCodeRecords, EtlBusImport etlBusImportRecord)
        {
            // Setup TPL Dataflow blocks
            var bufferBlock = new BufferBlock<ZipCodeDetails>();
            TransformBlock<ZipCodeDetails, ZipCodeDetails> transformBlock;
            ActionBlock<ZipCodeDetails> actionBlock;

            // Use concurrent bag in case action block uses multiple threads.
            var batchedZipCodeDetailsForUpserting = new ConcurrentBag<ZipCodeDetails>();

            var batchTimer = new Stopwatch();
            var totalProcessedUploadedRecordCount = 0;

            // Func for handling database upload.
            Func<ConcurrentBag<ZipCodeDetails>, Task> performDatabaseUpload = async (recordsForUploading) =>
            {
                if (recordsForUploading.Count > 0)
                {
                    using var etlBusDbContext = _etlBusDbContext == null ? new EtlBusDbContext(_appConfig) : _etlBusDbContext;
                    await etlBusDbContext.BulkInsertOrUpdateAsync(recordsForUploading).ConfigureAwait(false);
                    await etlBusDbContext.SaveChangesAsync().ConfigureAwait(false);
                    _logger.LogInformation($"Saved {recordsForUploading.Count} to db. Batch processing time: ${batchTimer.ElapsedMilliseconds} ms.");
                }
            };

            transformBlock = new TransformBlock<ZipCodeDetails, ZipCodeDetails>(
                async zipCodeRecord =>
                {
                    try
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
                    }
                    catch(Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to transform record with key {zipCodeRecord.CompositeKey}");
                    }
                    return zipCodeRecord;
                },
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = _processingSettings.TransformMaxDegreeOfParallelism });

            actionBlock = new ActionBlock<ZipCodeDetails>(
                async zipCodeRecord =>
                {
                    zipCodeRecord.CreationDateUtc = DateTime.UtcNow; // TODO: Do not overwrite this.
                    zipCodeRecord.LastModifiedDateUtc = DateTime.UtcNow;

                    try
                    {
                        batchedZipCodeDetailsForUpserting.Add(zipCodeRecord);

                        // Bulk save records.
                        if (batchedZipCodeDetailsForUpserting.Count >= _processingSettings.BatchRecordSaveCount)
                        {
                            await performDatabaseUpload(batchedZipCodeDetailsForUpserting).ConfigureAwait(false);
                            batchedZipCodeDetailsForUpserting.Clear();
                            batchTimer.Restart();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "There was a problem saving changes to the database.");
                    }
                },
                new ExecutionDataflowBlockOptions { BoundedCapacity = _processingSettings.ActionBoundedCapacity, MaxDegreeOfParallelism = _processingSettings.ActionMaxDegreesOfParallelism });

            // Link the blocks
            bufferBlock.LinkTo(transformBlock, new DataflowLinkOptions { PropagateCompletion = true });
            transformBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

            if (zipCodeRecords.Count > 0)
            {
                // Load zip codes into buffer block.
                foreach (var zipCodeRecord in zipCodeRecords)
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
                bufferBlock.Complete();
                batchTimer = Stopwatch.StartNew();

                try
                {
                    await Task.WhenAll(bufferBlock.Completion, transformBlock.Completion, actionBlock.Completion);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Dataflow terminated with an error.");
                }

                if (batchedZipCodeDetailsForUpserting.Count > 0)
                {
                    // Perform upload for any remaining records that fell outside of min. batch size.
                    await performDatabaseUpload(batchedZipCodeDetailsForUpserting).ConfigureAwait(false);
                    batchTimer.Stop();
                }
            }

            return totalProcessedUploadedRecordCount;
        }

        /// <summary>
        /// Finalizes import record in database.
        /// </summary>
        /// <param name="etlBusImportRecord"></param>
        /// <returns></returns>
        public async Task<EtlBusImport?> SetImportRecordCompleteAsync(EtlBusImport? etlBusImportRecord, ProcessingStatus terminalStatus = ProcessingStatus.Complete)
        {
            using var etlBusDbContext = _etlBusDbContext == null ? new EtlBusDbContext(_appConfig) : _etlBusDbContext;
            etlBusImportRecord = await etlBusDbContext.EtlBusImports.FirstOrDefaultAsync(i => i.Id == etlBusImportRecord.Id).ConfigureAwait(false);
            if (etlBusImportRecord != null)
            {
                etlBusImportRecord.Status = terminalStatus.ToString();
                etlBusImportRecord.EndDateTimeUtc = DateTime.UtcNow;
                await etlBusDbContext.SaveChangesAsync().ConfigureAwait(false);
            }
            else
            {
                _logger.LogCritical("Could not finalize import record.");
            }

            return etlBusImportRecord;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_etlBusDbContext != null)
                {
                    _etlBusDbContext.DisposeAsync().GetAwaiter().GetResult();
                }
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
