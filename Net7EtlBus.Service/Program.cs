using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Net7EtlBus.Service.Core.Concretes;
using Net7EtlBus.Service.Core.Interfaces;
using Net7EtlBus.Service.Models;

namespace Net7EtlBus.Service
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<ServiceBusWorker>();
                    services.AddHttpClient<IGoogleApiService, GoogleApiService>();
                    services.AddTransient<IDataflowProcessor, DataflowProcessor>();
                    services.AddTransient(sp => new Lazy<IDataflowProcessor>(() => sp.GetRequiredService<IDataflowProcessor>()));
                    services.Configure<ProcessingSettings>(hostContext.Configuration.GetSection("ProcessingSettings"));
                });
    }
}
