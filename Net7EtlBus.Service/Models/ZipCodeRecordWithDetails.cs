namespace Net7EtlBus.Models
{
    public class ZipCodeRecordWithDetails : ZipCodeRecordBase
    {
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public double? Elevation { get; set; }
        public string? Timezone { get; set; }
        public DateTime CreationDate { get; set; }
        public DateTime LastModifiedDate { get; set; }
    }
}
