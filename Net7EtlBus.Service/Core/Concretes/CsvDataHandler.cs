using CsvHelper;
using Net7EtlBus.Service.Core.Interfaces;
using System.Globalization;

namespace Net7EtlBus.Service.Core.Concretes
{
    public class CsvDataHandler : IFileDataHandler
    {
        /// <summary>
        /// Read records from CSV file.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public IEnumerable<T> GetRecords<T>(string filePath)
        {
            using var reader = new StreamReader(filePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<T>().ToList();

            return records;
        }
    }
}
