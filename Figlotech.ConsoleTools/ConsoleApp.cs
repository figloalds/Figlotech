using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.ConsoleTools
{
    public abstract class ConsoleApp
    {
        public abstract void Run();

        internal static String[] SimpleParse(String inputStr) {
            String[] input = inputStr.Split(' ');
            List<String> li = new List<String>();
            Boolean AppendingString = false;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < input.Length; i++) {
                if (AppendingString) {
                    sb.Append(" " + input[i].Replace("\"", ""));
                }
                if (input[i].Contains("\"")) {
                    input[i] = input[i].Replace("\"", "");
                    AppendingString = !AppendingString;
                    if (!AppendingString) {
                        li.Add(sb.ToString());
                        sb = new StringBuilder();
                        continue;
                    }
                    else {
                        sb.Append(input[i]);
                    }
                }
                if (AppendingString) {
                    continue;
                }
                li.Add(input[i]);
            }
            return li.ToArray();
        }

        private static void WriteAt(int left, int top, string txt) {
            lock ("CONSOLE_WRITE") {
                System.Console.SetCursorPosition(left, top);
                System.Console.Write(txt);
            }
        }
    }
}
