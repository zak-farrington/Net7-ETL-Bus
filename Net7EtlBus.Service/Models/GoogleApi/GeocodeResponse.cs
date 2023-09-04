using System.Text.Json.Serialization;

namespace Net7EtlBus.Models.GoogleApi
{
    public class GeocodeResponse : GoogleApiResponseBase
    {
        public List<GeocodeResult> Results { get; set; }

        public double? Latitude => Results?.FirstOrDefault()?.Geometry?.Location?.Latitude;
        public double? Longitude => Results?.FirstOrDefault()?.Geometry?.Location?.Longitude;

        public override bool IsSuccessful => string.IsNullOrEmpty(ErrorMessage) && Latitude.HasValue && Longitude.HasValue;
    }

    public class Coordinates
    {
        [JsonPropertyName("lat")]
        public double Latitude { get; set; }

        [JsonPropertyName("lng")]
        public double Longitude { get; set; }
    }

    public class Geometry
    {
        public Coordinates Location { get; set; }
    }

    public class GeocodeResult
    {
        public Geometry Geometry { get; set; }
    }
}
