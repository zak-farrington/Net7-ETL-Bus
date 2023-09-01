
using System.Text.Json.Serialization;

namespace Net7EtlBus.Models
{
    public class ZipCodeRecordBase
    {
        [JsonPropertyName("state_abbr")]
        public required string StateCode { get; set; }
        public required string ZipCode { get; set; }
        public required string County { get; set; }
        public required string City { get; set; }
    }
}
