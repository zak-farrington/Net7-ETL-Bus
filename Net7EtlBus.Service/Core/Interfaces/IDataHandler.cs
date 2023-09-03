namespace Net7EtlBus.Service.Core.Interfaces
{
    public interface IDataHandler
    {
        IEnumerable<T> GetRecords<T>(string filePath);
    }
}
