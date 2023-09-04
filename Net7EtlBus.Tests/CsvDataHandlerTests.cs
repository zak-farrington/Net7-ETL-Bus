using Net7EtlBus.Models;

namespace Net7EtlBus.Tests.CsvDataHandler
{
    public class Integration
    {
        private readonly string _csvFileName = "test_csv_file.csv";
        private readonly string _csvSampleText = @"state_fips,state,state_abbr,zipcode,county,city
1,Alabama,AL,35004,St.Clair,Acmar";

        [Fact]
        public void GetRecords_ShouldReadRecords_Success()
        {
            CreateSampleCsvFile();

            var csvDataHandler = new Service.Core.Concretes.CsvDataHandler();
            var zipRecords = csvDataHandler.GetRecords<ZipCodeRecord>(_csvFileName).ToList();

            Assert.Single(zipRecords);
            Assert.Equal("35004", zipRecords[0].ZipCode);
            Assert.Equal("AL", zipRecords[0].StateCode);
            Assert.Equal("St.Clair", zipRecords[0].County);
            Assert.Equal("Acmar", zipRecords[0].City);

            Service.Utilities.FileSystem.DeleteFile(_csvFileName);
        }

        private void CreateSampleCsvFile()
        {
            File.WriteAllText(_csvFileName, _csvSampleText);
        }
    }
}
