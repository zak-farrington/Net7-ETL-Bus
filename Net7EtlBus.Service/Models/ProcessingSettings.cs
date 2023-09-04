using Net7EtlBus.Service.Utilities;

namespace Net7EtlBus.Service.Models
{
    public class ProcessingSettings
    {
        public int ValidRecordDaysTtl { get; set; } = Constants.DefaultProcessingSettings.ValidRecordDaysTtl;
        public int TransformMaxDegreeOfParallelism { get; set; } = Constants.DefaultProcessingSettings.TransformMaxDegreeOfParallelism;
        public int ActionMaxDegreesOfParallelism { get; set; } = Constants.DefaultProcessingSettings.ActionMaxDegreesOfParallelism;
        public int ActionBoundedCapacity { get; set; } = Constants.DefaultProcessingSettings.ActionBoundedCapacity;
        public int BatchRecordSaveCount { get; set; } = Constants.DefaultProcessingSettings.BatchRecordSaveCount;
    }
}
