using System.Text.Json.Serialization;

namespace Net7EtlBus.Models.GoogleApi
{
    public class GeocodeResponse : GoogleApiResponseBase
    {
        public List<GeocodeResult> Results { get; set; }
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
