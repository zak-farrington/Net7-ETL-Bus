using Microsoft.Extensions.Configuration;
using System.Text.Json;
using Net7EtlBus.Models.GoogleApi;

public class GoogleApiService : IGoogleApiService
{
    private readonly HttpClient _httpClient;
    private readonly IConfiguration _appConfig;
    private readonly string _googleApiKey;

    private readonly string _googleMapsApiRoot = "https://maps.googleapis.com/maps/api/";

    private readonly JsonSerializerOptions _jsonSerializerOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    public GoogleApiService(HttpClient httpClient, IConfiguration appConfig)
    {
        _httpClient = httpClient;
        _appConfig = appConfig;
        _googleApiKey = _appConfig["GoogleApiKey"] ?? throw new InvalidOperationException("Google API key is missing.");
    }

    /// <summary>
    /// Handle Google API GET calls via HttpClient, and handle errors
    /// TODO: Add rate limiting.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="apiEndpoint"></param>
    /// <returns></returns>
    private async Task<T> GetGoogleApiResponseAsync<T>(string apiEndpoint) where T : GoogleApiResponseBase, new()
    {
        var response = new T();

        var fullUrl = $"{_googleMapsApiRoot}{apiEndpoint}";
        try
        {
            var httpResponse = await _httpClient.GetAsync(fullUrl).ConfigureAwait(false);

            if (httpResponse.IsSuccessStatusCode)
            {
                var responseContent = await httpResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
                response = JsonSerializer.Deserialize<T>(responseContent, _jsonSerializerOptions);
            }
            else
            {
                response.ErrorMessage = $"HTTP Status {httpResponse.StatusCode} encountered.";
            }
        }
        catch (Exception ex)
        {
            response.ErrorMessage = ex.Message;
        }

        return response;
    }

    /// <summary>
    /// Retrieve Lat and Lng from Google maps.
    /// </summary>
    /// <param name="zipCode"></param>
    /// <returns></returns>
    public async Task<GeocodeResponse> GetLatLngFromZipAsync(string zipCode)
    {
        var url = $"geocode/json?address={zipCode}&result_type=postal_code&location_type=ROOFTOP&key={_googleApiKey}";
        var response = await GetGoogleApiResponseAsync<GeocodeResponse>(url).ConfigureAwait(false);
        return response;
    }

    public async Task<ElevationResponse> GetElevationAsync(string latLng)
    {
        var url = $"elevation/json?locations={latLng}&key={_googleApiKey}";
        var response = await GetGoogleApiResponseAsync<ElevationResponse>(url).ConfigureAwait(false);
        return response;
    }

    public async Task<TimeZoneResponse> GetTimeZoneAsync(string latLng)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var url = $"timezone/json?location={latLng}&timestamp={timestamp}&key={_googleApiKey}";
        var response = await GetGoogleApiResponseAsync<TimeZoneResponse>(url).ConfigureAwait(false);
        return response;
    }
}
