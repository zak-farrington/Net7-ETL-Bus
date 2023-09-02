
using CsvHelper.Configuration.Attributes;

namespace Net7EtlBus.Models
{
    public class ZipCodeRecord
    {
        [Name("state")]
        public required string State { get; set; }
        [Name("state_abbr")]
        public required string StateCode { get; set; }
        [Name("zipcode")]
        public virtual required string ZipCode { get; set; }
        [Name("county")]
        public required string County { get; set; }
        [Name("city")]
        public required string City { get; set; }
    }
}
