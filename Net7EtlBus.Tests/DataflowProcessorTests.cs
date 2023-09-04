using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Moq.EntityFrameworkCore;
using Net7EtlBus.Models;
using Net7EtlBus.Models.GoogleApi;
using Net7EtlBus.Service.Core.Concretes;
using Net7EtlBus.Service.Data;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Models.EtlBusDb;
using Net7EtlBus.Service.Utilities;

namespace Net7EtlBus.Tests.DataFlowProcessorTests
{
    public class Unit
    {
        private readonly DataflowProcessor _dataflowProcessor;
        private readonly Mock<IConfiguration> _mockConfig = new Mock<IConfiguration>();
        private readonly Mock<ILogger<DataflowProcessor>> _mockLogger = new Mock<ILogger<DataflowProcessor>>();

        private readonly EtlBusImport _etlBusImport = new EtlBusImport
        {
            Id = 1,
            ImportStartTimeUtc = DateTime.UtcNow,
            IsActive = true,
            FileName = Constants.GeoDataCsvFileName,
            Status = nameof(Constants.ProcessingStatus.Running),
        };

        public Unit()
        {
            var processingSettings = new ProcessingSettings();

            var mockGoogleApiService = SetupAndMockGoogleApiServiceMethods();

            var mockIOptions = Options.Create(processingSettings);
            
            var mockDbContext = SetupAndGetMockEtlBusDbContext();

            _dataflowProcessor = new DataflowProcessor(
                _mockConfig.Object,
                _mockLogger.Object,
                mockGoogleApiService.Object,
                mockIOptions,
                mockDbContext.Object
            );
        }

        /// <summary>
        /// Setup mock responses for GoogleApiService calls.
        /// </summary>
        private Mock<IGoogleApiService> SetupAndMockGoogleApiServiceMethods()
        {
            var mockGoogleApiService = new Mock<IGoogleApiService>();

            var mockGeocodeResponse = new GeocodeResponse
            {
                Results = new List<GeocodeResult>
                {
                    new GeocodeResult
                    {
                        Geometry = new Geometry
                        {
                            Location = new Coordinates { Latitude = 31.2562, Longitude = -85.6229 }
                        }
                    }
                }
            };

            mockGoogleApiService.Setup(s => s.GetLatLngFromZipAsync(It.IsAny<string>()))
                                 .Returns(Task.FromResult(mockGeocodeResponse));

            var mockElevationResponse = new ElevationResponse
            {
                Results = new List<ElevationResult>
                {
                    new ElevationResult { Elevation = 101.1, Resolution = 0.1 }
                },
                    Status = "OK"
                };

            mockGoogleApiService.Setup(s => s.GetElevationAsync(It.IsAny<string>()))
                                 .Returns(Task.FromResult(mockElevationResponse));

            var mockTimeZoneResponse = new TimeZoneResponse
            {
                DstOffset = 3500,
                RawOffset = 0,
                Status = "OK",
                TimeZoneName = "Central Daylight Time"
            };

            mockGoogleApiService.Setup(s => s.GetTimeZoneAsync(It.IsAny<string>()))
                                 .Returns(Task.FromResult(mockTimeZoneResponse));

            return mockGoogleApiService;
        }

        /// <summary>
        /// Setup mock db context and BulkInsertOrUpdateAsync.
        /// </summary>
        /// <returns></returns>
        private Mock<EtlBusDbContext> SetupAndGetMockEtlBusDbContext()
        {
            // Add a valid ZipCodeDetails record to our mock db.
            var mockZipCodeDetails = GetMockZipCodeDetails("35004", "AL", "Alabama", "Acmad", "St.Clair", 10, 10, 10, "Central", DateTime.UtcNow, DateTime.UtcNow);
            var mockDbContext = CreateMockDbContext(mockZipCodeDetails);

            // TODO: Cannot moq BulkInsertOrUpdateAsync due to library limitations.  Figure out work around or create integration test instead.
            //mockDbContext.Setup(db => db.BulkInsertOrUpdateAsync(
            //    It.IsAny<IList<ZipCodeDetails>>(),
            //    It.IsAny<BulkConfig?>(),
            //    It.IsAny<Action<decimal>?>(),
            //    It.IsAny<Type?>(),
            //    It.IsAny<CancellationToken>()
            //    )).Returns(Task.FromResult(true));

            return mockDbContext;
        }

        /// <summary>
        /// Create mock db context with optional zip code record.
        /// </summary>
        /// <param name="_mockBook"></param>
        /// <returns></returns>
        private Mock<EtlBusDbContext> CreateMockDbContext(ZipCodeDetails? zipCodeRecord)
        {
            var zipcodeRecords = new List<ZipCodeDetails> { };
            if (zipcodeRecords != null)
            {
                zipcodeRecords.Add(zipCodeRecord);
            }

            var etlBusImportRecords = new List<EtlBusImport> { _etlBusImport };

            var mockContext = new Mock<EtlBusDbContext>();
            mockContext.Setup(x => x.ZipCodeDetails).ReturnsDbSet(zipcodeRecords);
            mockContext.Setup(x => x.EtlBusImports).ReturnsDbSet(etlBusImportRecords);

            return mockContext;
        }

        /// <summary>
        /// Helper to get ZipCodeRecord.
        /// </summary>
        /// <param name="zipCode"></param>
        /// <param name="stateCode"></param>
        /// <param name="state"></param>
        /// <param name="city"></param>
        /// <param name="county"></param>
        /// <returns></returns>
        private static ZipCodeRecord GetMockZipCodeRecord(string zipCode, string stateCode, string state, string city, string county)
        {
            return new ZipCodeRecord
            {
                ZipCode = zipCode,
                StateCode = stateCode,
                State = state,
                City = city,
                County = county,
            };
        }

        /// <summary>
        /// Helper to get ZipCodeDetails.
        /// </summary>
        /// <param name="zipCode"></param>
        /// <param name="stateCode"></param>
        /// <param name="state"></param>
        /// <param name="city"></param>
        /// <param name="county"></param>
        /// <param name="latitude"></param>
        /// <param name="longitude"></param>
        /// <param name="elevation"></param>
        /// <param name="timeZone"></param>
        /// <param name="creationDateUtc"></param>
        /// <param name="lastModifiedDateUtc"></param>
        /// <returns></returns>
        private static ZipCodeDetails GetMockZipCodeDetails(string zipCode, string stateCode, string state, string city, string county, double? latitude, double? longitude, double? elevation, string timeZone, DateTime creationDateUtc, DateTime lastModifiedDateUtc)
        {
            return new ZipCodeDetails
            {
                CompositeKey = ZipCodeHelpers.GetCompositeKey(zipCode, stateCode),
                ZipCode = zipCode,
                StateCode = stateCode,
                State = state,
                City = city,
                County = county,
                Latitude = latitude,
                Longitude = longitude,
                Elevation = elevation,
                Timezone = timeZone,
                CreationDateUtc = creationDateUtc,
                LastModifiedDateUtc = lastModifiedDateUtc,
            };
        }

        [Fact]
        public async void EvaluateEtlRunConditionsAsync_FileNameEmpty_ShouldNotRun()
        {
            string fileName = string.Empty;

            var result = await _dataflowProcessor.EvaluateEtlRunConditionsAsync(fileName).ConfigureAwait(false);

            Assert.False(result.ShouldRun);
        }

        [Fact]
        public void GetRecordsExcludingPreviouslyProcessed_NoneExcluded_Success()
        {
            // Zip code reflects different then what is already in our mock db, so nothing should be excluded.
            var mockZipCodeRecord = GetMockZipCodeRecord("75074", "TX", "Texas", "Plano", "Collin");

            var sampleZipCodeData = new Dictionary<string, ZipCodeRecord>
            {
                { ZipCodeHelpers.GetCompositeKey(mockZipCodeRecord.ZipCode, mockZipCodeRecord.StateCode), mockZipCodeRecord }
            };

            var result = _dataflowProcessor.GetRecordsExcludingPreviouslyProcessed(sampleZipCodeData);

            Assert.Single(result);
        }

        //[Fact]
        //public void GetRecordsExcludingPreviouslyProcessed_RecordExcluded_Success()
        //{
        //    // Zip code reflects record that is already valid in our mock db, so it should be excluded.
        //    var mockZipCodeRecord = GetMockZipCodeRecord("35004", "AL", "Alabama", "Acmad", "St.Clair");

        //    var sampleZipCodeData = new Dictionary<string, ZipCodeRecord>
        //    {
        //        { ZipCodeHelpers.GetCompositeKey(mockZipCodeRecord.ZipCode, mockZipCodeRecord.StateCode), mockZipCodeRecord }
        //    };

        //    var result = _dataflowProcessor.GetRecordsExcludingPreviouslyProcessed(sampleZipCodeData);

        //    Assert.Empty(result);
        //}

        [Fact]
        public async void SetImportRecordCompleteAsync_MarkImportRecordComplete_Success()
        {
            var etlBusImport = await _dataflowProcessor.SetImportRecordCompleteAsync(_etlBusImport, Constants.ProcessingStatus.Complete);

            Assert.Equal(nameof(Constants.ProcessingStatus.Complete), etlBusImport.Status);
            Assert.NotNull(etlBusImport.EndDateTimeUtc);
        }
    }
}
