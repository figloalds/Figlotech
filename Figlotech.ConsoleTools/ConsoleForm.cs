using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Figlotech.ConsoleUtils
{
    public abstract class ConsoleForm
    {
        List<IConsoleControl> controls = new List<IConsoleControl>();

        private static void WriteAt(int left, int top, string txt) {
            lock ("CONSOLE_WRITE") {
                System.Console.SetCursorPosition(left, top);
                System.Console.Write(txt);
            }
        }
    }
}
