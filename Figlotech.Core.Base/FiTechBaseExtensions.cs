using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core
{
    public static class FiTechBaseExtensions
    {
        public static SelfInitializerDictionary<string, bool> EnabledSystemLogs { get; private set; } = new SelfInitializerDictionary<string, bool>((str) => false);
        public static void WriteLine(this Fi _selfie, string s = "") {
            WriteLineInternal(_selfie, "FTH:Generic", () => s);
        }
        public static void WriteLine(this Fi _selfie, string origin, string s) {
            WriteLineInternal(_selfie, origin, () => s);
        }

        internal static void WriteLineInternal(this Fi _selfie, string origin, Func<string> s) {
            lock (FTHLogStream) {
                if (EnableStdoutLogs && EnabledSystemLogs[origin]) {
                    FTHLogStream.WriteLine($"[{origin}] {s()}");
                    FTHLogStream.Flush();
                }
            }
        }

    }
}
