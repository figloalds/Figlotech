using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting
{
    public static class IFthCyclicServiceExtensions {
        public static void Break(this IFthCyclicService me) {
            me.BreakMainLoop = true;
        }
    }
    public interface IFthCyclicService : IFthService {
        bool BreakMainLoop { get; set; }
        Task MainLoopInit();
        Task MainLoopIteration();
        int IterationDelay { get; }
    }
}
