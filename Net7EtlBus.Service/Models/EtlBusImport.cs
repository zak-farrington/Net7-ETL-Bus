using static Net7EtlBus.Service.Utilities.Constants;

namespace Net7EtlBus.Service.Models
{
    public class EtlBusImport
    {
        public int Id { get; set; }
        public required string FileName { get; set; }
        public required string FileChecksum { get; set; }
        public bool IsActive { get; set; } 
        public string Status { get; set; }

        public DateTime? ImportStartTimeUtc { get; set; }
        public DateTime? EndDateTimeUtc { get; set;}
    }
}
