﻿using Net7EtlBus.Models;
using Net7EtlBus.Service.Utilities;
using System.ComponentModel.DataAnnotations;

namespace Net7EtlBus.Service.Models.EtlBusDb
{
    public class ZipCodeDetails : ZipCodeRecord
    {
        [Key]
        public required string CompositeKey { get; set; }
        public override required string ZipCode { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public double? Elevation { get; set; }
        public string? Timezone { get; set; }
        public DateTime CreationDateUtc { get; set; }
        public DateTime LastModifiedDateUtc { get; set; }
        public int? ImportId { get; set; }
    }
}
