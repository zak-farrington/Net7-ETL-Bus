using Net7EtlBus.Models.GoogleApi;

public interface IGoogleApiService
{
    Task<GeocodeResponse> GetLatLngFromZipAsync(string zipCode);
    Task<ElevationResponse> GetElevationAsync(string latLng);
    Task<TimeZoneResponse> GetTimeZoneAsync(string latLng);
}