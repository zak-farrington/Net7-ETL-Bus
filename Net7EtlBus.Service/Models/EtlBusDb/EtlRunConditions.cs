using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Net7EtlBus.Service.Models.EtlBusDb
{
    public class EtlRunConditions
    {
        public bool ShouldRun { get; set; }
        public string FileNameForProcessing { get; set; }
        public EtlBusImport EtlBusImport { get; set; }

    }
}
