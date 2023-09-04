using System.Text.Json.Serialization;

namespace Net7EtlBus.Models.GoogleApi
{
    public class GoogleApiResponseBase
    {
        // Google APIs will return error_message if there's a problem.
        [JsonPropertyName("error_message")]
        public string? ErrorMessage { get; set; }
        public virtual bool IsSuccessful => string.IsNullOrEmpty(ErrorMessage);
    }
}
