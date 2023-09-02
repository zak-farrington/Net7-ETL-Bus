using System.Text.Json.Serialization;

namespace Net7EtlBus.Models.GoogleApi
{
    public class ElevationResponse : GoogleApiResponseBase
    {
        public List<ElevationResult> Results { get; set; }
        public string Status { get; set; }

        public double? Elevation => Results?.FirstOrDefault()?.Elevation;
    }

    public class ElevationResult
    {
        public double Elevation { get; set; }

        public double Resolution { get; set; }
    }
}