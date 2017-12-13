using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting
{
    public class FthServiceStatus {
        public bool IsRunning { get; set; }
        public bool CanStart { get; set; }
        public string StatusText { get; set; }
    }
    public interface IFthService
    {
        Thread Thread { get; set; }
        Dictionary<string, object> Configuration { get; }
        FthServiceStatus GetStatus();
        void Init(params string[] args);

        string Exec(string command);

        void Run();
    }
}
