﻿using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Models.EtlBusDb;

namespace Net7EtlBus.Service.Data
{
    public class EtlBusDbContext : DbContext
    {
        private readonly IConfiguration? _appConfig;

        public EtlBusDbContext()
        {

        }

        public EtlBusDbContext(IConfiguration appConfig)
        {
            _appConfig = appConfig;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            var connectionStrings = _appConfig.GetConnectionString("EtlBusDb.PostgresSQL");
            options.UseNpgsql(connectionStrings);
        }

        public virtual DbSet<EtlBusImport> EtlBusImports { get; set; }
        public virtual DbSet<ZipCodeDetails> ZipCodeDetails { get; set; }
    }
}
