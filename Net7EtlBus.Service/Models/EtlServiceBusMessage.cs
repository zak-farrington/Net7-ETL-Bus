namespace Net7EtlBus.Service.Models
{
    public class EtlServiceBusMessage
    {
        /// <summary>
        /// Force process to re run even if it has already processed an import.
        /// </summary>
        public bool ForceRun { get; set; }
    }
}
