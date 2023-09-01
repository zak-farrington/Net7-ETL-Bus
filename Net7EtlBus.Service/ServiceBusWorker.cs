using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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

            _sbConnectionString = _appConfig["ServiceBusConnectionPrimary"] ?? throw new InvalidOperationException("ServiceBusConnectionPrimary API key is missing.");  ;
            _sbQueueName = _appConfig["ServiceBusQueueName"] ?? throw new InvalidOperationException("ServiceBusQueue name is is missing."); ;
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

                // Testing Google API calls
                var geocodeResult = _googleApiService.GetLatLngFromZipAsync("75074").ConfigureAwait(false);
                var elevationResult = _googleApiService.GetElevationAsync("33.03884,-96.67092").ConfigureAwait(false);
                var timeZoneResult = _googleApiService.GetTimeZoneAsync("33.03884,-96.67092").ConfigureAwait(false);

                // TODO:
                // Step 1: Load CSV file of zip codes (possibly from FTP)
                // Step 2: TPL Dataflow to Google APIs in batch
                // Step 3: Store in DB via Entity framework
            }
            catch (Exception ex)
            {
                _logger.LogError("Unhandled error has been countered while processing message.");
                throw ex;
            }
            finally
            {
                _logger.LogInformation("Message has been completed.");
                await args.CompleteMessageAsync(args.Message);
            }
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "Service bus error handler encountered exception.");
            return Task.CompletedTask;
        }
    }
}
