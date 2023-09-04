namespace Net7EtlBus.Service.Utilities
{
    public static class Constants
    {
        public enum ProcessingStatus
        {
            None,
            Running,
            Error,
            Complete
        }

        public static class DefaultProcessingSettings
        {
            public const int ValidRecordDaysTtl = 30;
            public const int TransformMaxDegreeOfParallelism = 5;
            public const int ActionMaxDegreesOfParallelism = 1;
            public const int ActionBoundedCapacity = 1;
            public const int BatchRecordSaveCount = 25;
        }
    }
}
