namespace Net7EtlBus.Models.GoogleApi
{
    public class TimeZoneResponse : GoogleApiResponseBase
    {
        public int DstOffset { get; set; }
        public int RawOffset { get; set; }
        public string Status { get; set; }
        public string TimeZoneId { get; set; }
        public string TimeZoneName { get; set; }

        public override bool IsSuccessful => string.IsNullOrEmpty(ErrorMessage) && !string.IsNullOrEmpty(TimeZoneName);
    }
}
