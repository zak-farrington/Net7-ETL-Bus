using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Moq.EntityFrameworkCore;
using Net7EtlBus.Models;
using Net7EtlBus.Service.Core.Concretes;
using Net7EtlBus.Service.Data;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Models.EtlBusDb;
using Net7EtlBus.Service.Utilities;

namespace Net7EtlBus.Tests.DataFlowProcessorTests
{
    public class UnitTests
    {
        private readonly DataflowProcessor _dataflowProcessor;
        private readonly Mock<IConfiguration> _mockConfig = new Mock<IConfiguration>();
        private readonly Mock<ILogger<DataflowProcessor>> _mockLogger = new Mock<ILogger<DataflowProcessor>>();
        private readonly Mock<IGoogleApiService> _mockGoogleApiService = new Mock<IGoogleApiService>();

        private readonly ZipCodeRecord _mockZipCodeRecord = new ZipCodeRecord
        {
            State = "Alabama",
            StateCode = "AL",
            ZipCode = "35004",
            City = "Acmar",
            County = "St.Clair",
        };

        private readonly ZipCodeDetails _mockZipCodeDetails = new ZipCodeDetails
        {
            CompositeKey = ZipCodeHelpers.GetCompositeKey("35004", "AL"),
            State = "Alabama",
            StateCode = "AL",
            ZipCode = "35004",
            City = "Acmar",
            County = "St.Clair",
            CreationDateUtc = DateTime.UtcNow.AddDays(-1),
            LastModifiedDateUtc = DateTime.UtcNow.AddDays(-1),
        };

        public UnitTests()
        {
            var processingSettings = new ProcessingSettings();
            var mockIOptions = Options.Create(processingSettings);

            var mockDbContext = CreateMockContext(_mockZipCodeDetails);

            _dataflowProcessor = new DataflowProcessor(
                _mockConfig.Object,
                _mockLogger.Object,
                _mockGoogleApiService.Object,
                mockIOptions,
                mockDbContext.Object
            );
        }

        /// <summary>
        /// Helper to create mock db context. 
        /// </summary>
        /// <param name="_mockBook"></param>
        /// <returns></returns>
        private Mock<EtlBusDbContext> CreateMockContext(ZipCodeDetails? zipCodeRecord)
        {
            var zipcodeRecords = new List<ZipCodeDetails> { };
            if (zipcodeRecords != null)
            {
                zipcodeRecords.Add(zipCodeRecord);
            }

            var mockContext = new Mock<EtlBusDbContext>();
            mockContext.Setup(x => x.ZipCodeDetails).ReturnsDbSet(zipcodeRecords);
            return mockContext;
        }

        [Fact]
        public void EvaluateEtlRunConditionsAsync_FileNameEmpty_ShouldNotRun()
        {
            string fileName = string.Empty;

            var result = _dataflowProcessor.EvaluateEtlRunConditionsAsync(fileName).Result;

            Assert.False(result.ShouldRun);
        }

        [Fact]
        public void GetRecordsExcludingPreviouslyProcessed_ExcludesProcessed_Success()
        {
            var sampleZipCodeData = new Dictionary<string, ZipCodeRecord>
            {
                { ZipCodeHelpers.GetCompositeKey(_mockZipCodeRecord.ZipCode, _mockZipCodeRecord.StateCode), _mockZipCodeRecord }
            };

            var result = _dataflowProcessor.GetRecordsExcludingPreviouslyProcessed(sampleZipCodeData);


            Assert.True(result.Count == 0);
        }


        //[Fact]
        //public async Task InitializeAndExecuteDataflowAsync()
        //{
        //}


        //[Fact]
        //public void SetImportRecordCompleteAsync()
        //{

        //}
    }
}
