using Microsoft.Extensions.Configuration;

namespace Net7EtlBus.Tests.GoogleApiServiceTests
{
    public class Integration
    {
        private readonly IConfiguration _appConfig;
        private readonly HttpClient _httpClient;
        private readonly IGoogleApiService _googleApiService;

        private readonly string _zipCode = "75074";
        private readonly string _latLng = "33.0118,-96.6946";

        public Integration()
        {
            _appConfig = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            _httpClient = new HttpClient();
            _googleApiService = new GoogleApiService(_httpClient, _appConfig);
        }

        [Fact]
        public async Task GetLatLngFromZipAsync_ReturnsLatLng_Success()
        {
            var result = await _googleApiService.GetLatLngFromZipAsync(_zipCode).ConfigureAwait(false);

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Latitude);
            Assert.NotNull(result.Longitude);
        }

        [Fact]
        public async Task GetElevationAsync_ReturnsElevation_Success()
        {
            var result = await _googleApiService.GetElevationAsync(_latLng).ConfigureAwait(false); ;

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Elevation);
        }

        [Fact]
        public async Task GetTimeZoneAsync_ReturnsTimeZone_Success()
        {
            var result = await _googleApiService.GetTimeZoneAsync(_latLng).ConfigureAwait(false); ;

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.TimeZoneName);
        }

    }

}