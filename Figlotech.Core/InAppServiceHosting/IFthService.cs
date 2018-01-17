using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting
{
    
    public interface IFthService {
        bool InterruptIssued { get; set; }
        bool IsCritical { get; set; }

        void Init(params object[] args);

        string Exec(params object[] commands);

        void Run();

    }
}
