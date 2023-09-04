using Net7EtlBus.Models;
using Net7EtlBus.Service.Models;
using Net7EtlBus.Service.Models.EtlBusDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Net7EtlBus.Service.Utilities.Constants;

namespace Net7EtlBus.Service.Core.Interfaces
{
    public interface IDataflowProcessor
    {
        Task<EtlRunConditions> EvaluateEtlRunConditionsAsync(string fileName, bool forceRun = false);
        List<ZipCodeRecord> GetRecordsExcludingPreviouslyProcessed(Dictionary<string, ZipCodeRecord>? zipCodesPendingProcessingHashMap);
        Task<int> InitializeAndExecuteDataflowAsync(List<ZipCodeRecord> zipCodeRecords, EtlBusImport etlBusImportRecord);
        Task<EtlBusImport?> SetImportRecordCompleteAsync(EtlBusImport? etlBusImportRecord, ProcessingStatus terminalStatus = ProcessingStatus.Complete);
    }
}
