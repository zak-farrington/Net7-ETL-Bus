namespace Net7EtlBus.Service.Core.Interfaces
{
    /// <summary>
    /// Interface that can be implemented by CsvDataHandler.  
    /// Xml, Json or similar data handlers can be added in the future.
    /// </summary>
    public interface IFileDataHandler
    {
        IEnumerable<T> GetRecords<T>(string filePath);
    }
}
