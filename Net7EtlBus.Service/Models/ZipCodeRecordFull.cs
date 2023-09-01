namespace Net7EtlBus.Models
{
    public class ZipCodeRecordFull : ZipCodeRecordBase
    {
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public string? Elevation { get; set; }
        public string? Timezone { get; set; }
        public DateTime CreationDate { get; set; }
        public DateTime LastModifiedDate { get; set; }
    }
}
