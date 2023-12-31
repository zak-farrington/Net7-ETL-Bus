﻿using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Net7EtlBus.Models;
using Net7EtlBus.Service.Core.Concretes;
using Net7EtlBus.Service.Core.Interfaces;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Utilities;
using Newtonsoft.Json;
using System.Diagnostics;

namespace Net7EtlBus.Service
{
    public class ServiceBusWorker : BackgroundService
    {
        private readonly ILogger<ServiceBusWorker> _logger;
        private readonly IConfiguration _appConfig;

        private readonly string _sbConnectionString;
        private readonly string _sbQueueName;

        private readonly ServiceBusClient _serviceBusClient;
        private readonly ServiceBusProcessor _serviceBusProcessor;
        
        private readonly Lazy<IFileDataHandler> _dataHandlerLazy;
        private readonly Lazy<IDataflowProcessor> _dataFlowProcessorLazy;

        public ServiceBusWorker(ILogger<ServiceBusWorker> logger, IConfiguration appConfig, Lazy<IDataflowProcessor> dataFlowProcessor)
        {
            _logger = logger;
            _appConfig = appConfig;
            _dataHandlerLazy = new Lazy<IFileDataHandler>(() => new CsvDataHandler());
            _dataFlowProcessorLazy = dataFlowProcessor;

            _sbConnectionString = _appConfig["ServiceBus.ConnectionPrimary"] ?? throw new InvalidOperationException("ServiceBusConnectionPrimary API key is missing.");
            _sbQueueName = _appConfig["ServiceBus.QueueName"] ?? throw new InvalidOperationException("ServiceBusQueue name is is missing.");

            _serviceBusClient = new ServiceBusClient(_sbConnectionString);
            _serviceBusProcessor = _serviceBusClient.CreateProcessor(_sbQueueName, new ServiceBusProcessorOptions());
        }

        /// <summary>
        /// Entry point for BackgroundService.
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Background worker started.");

            // Configure the message and error handlers
            _serviceBusProcessor.ProcessMessageAsync += ProcessServiceBusMessageAsync;
            _serviceBusProcessor.ProcessErrorAsync += ErrorHandler;

            // Start processing
            await _serviceBusProcessor.StartProcessingAsync(stoppingToken);
            _logger.LogInformation("Service Bus listener has been started.");

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        /// <summary>
        /// Clean up when service is stopped.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            // Call your DisposeAsync method or directly put your cleanup code here
            if (_serviceBusClient != null)
            {
                await _serviceBusClient.DisposeAsync();
            }
            if (_serviceBusProcessor != null)
            {
                await _serviceBusProcessor.StopProcessingAsync();
                await _serviceBusProcessor.DisposeAsync();
            }
        }

        /// <summary>
        /// Event listener for service bus messages.
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private async Task ProcessServiceBusMessageAsync(ProcessMessageEventArgs args)
        {
            Stopwatch timer = new Stopwatch(); 
            timer.Start(); 

            var etlServiceBusMessage = JsonConvert.DeserializeObject<EtlServiceBusMessage>(args.Message.Body?.ToString());
            var forceRun = etlServiceBusMessage?.ForceRun ?? false;
            _logger.LogInformation($"Service Bus message receieved. Force run is: {forceRun}");

            var etlBusImportRecord = new EtlBusImport();

            try
            {
                // Step 1 - check if there is a file for processing.
                // TODO: Download from FTP and extract 
                var csvFileName = Constants.GeoDataCsvFileName;

                var etlRunConditions = await _dataFlowProcessorLazy.Value.EvaluateEtlRunConditionsAsync(csvFileName, forceRun).ConfigureAwait(false);
                if (!etlRunConditions.ShouldRun)
                {
                    _logger.LogError("Unable to continue. This run will abort.");
                    return;
                }

                // Record must be updated once we have finished processing.
                etlBusImportRecord = etlRunConditions.EtlBusImport;

                // Step 2 - read records from CSV
                var zipCodesPendingProcessingHashMap = _dataHandlerLazy.Value.GetRecords<ZipCodeRecord>(csvFileName)?.ToDictionary(zr => ZipCodeHelpers.GetCompositeKey(zr.ZipCode, zr.StateCode), zr => zr);

                var parsedRecordCount = zipCodesPendingProcessingHashMap?.Keys.Count;
                var hasParsedRecords = parsedRecordCount > 0;

                if (!hasParsedRecords)
                {
                    // Error encountered or no mappings to process.
                    _logger.LogError("Unable to continue. No mappings parsed from CSV. This run will abort.");
                    return;
                }
                else
                {
                    _logger.LogInformation($"Found ${parsedRecordCount} records in {csvFileName}");

                    // Step 3 - exclude any records that do not need to be processed.
                    var recordsForProcessing = _dataFlowProcessorLazy.Value.GetRecordsExcludingPreviouslyProcessed(zipCodesPendingProcessingHashMap);

                    // Step 4 - begin TPL data flow to transform & save records.
                    await _dataFlowProcessorLazy.Value.InitializeAndExecuteDataflowAsync(recordsForProcessing, etlBusImportRecord);

                    // Step 5 - mark import record as completed and perform final clean up.
                    await _dataFlowProcessorLazy.Value.SetImportRecordCompleteAsync(etlBusImportRecord).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Unhandled error has been countered while processing message.");
                if (etlBusImportRecord?.Id > 0)
                {
                    // We have a EtlBusImport record, so let's mark it with an error status.
                    await _dataFlowProcessorLazy.Value.SetImportRecordCompleteAsync(etlBusImportRecord, Constants.ProcessingStatus.Error).ConfigureAwait(false);
                }
                throw ex;
            } 
            finally
            {
                timer.Stop();
                _logger.LogInformation($"Message has been completed. Processing time: {timer.ElapsedMilliseconds} ms");
                await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
            }
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "Service bus error handler encountered exception.");
            return Task.CompletedTask;
        }
    }
}
