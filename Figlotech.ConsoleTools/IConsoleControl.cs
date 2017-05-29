using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.ConsoleUtils
{
    public interface IConsoleControl {
        int Top { get; set; }
        int Left { get; set; }
        int Width { get; set; }
        int Height { get; set; }

        void Render();
    }
}
