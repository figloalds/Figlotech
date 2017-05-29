using Figlotech.Autokryptex.EncryptMethods;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace Figlotech.CLI
{
    class Program
    {
        public static String ScriptKey = "KSPCOGURGGGDS94Y6QDDRKCHHE9A68LQG89TIQHCK65H000LYUECBHE4JDWUKP34";
        public static bool batchMode;
        static void Main(string[] args) {
            if (args.Length > 0 && File.Exists(args[0])) {
                var text = File.ReadAllText(args[0]);
                var bytes = Convert.FromBase64String(text);
                var decrypt = new AutokryptexEncryptor(ScriptKey).Decrypt(bytes);
                var script = Encoding.UTF8.GetString(decrypt);
                script = script.Replace("\r", "");
                var commands = script.Split('\n');
                foreach (var cmd in commands) {
                    try {
                        Exec(cmd, typeof(Toolset));
                    } catch (Exception x) {
                        Console.WriteLine("Script execution failed");
                        Environment.Exit(1);
                        return;
                    }
                }
                //File.Delete("script.esl");
                Console.WriteLine("Script Executed OK");
                Environment.Exit(0);
                return;
            }
            if (args.Contains("-b"))
                batchMode = true;
            var ver = Assembly.GetExecutingAssembly().GetName().Version;
            Console.WriteLine($"------------------------------------------------");
            Console.WriteLine($"-- FIGLOTECH CLI v{ver.Major}.{ver.Minor}.{ver.Build}");
            Console.WriteLine($"------------------------------------------------");
            MainLoop();
        }

        public static bool isLoggedIn = false;

        static void Exec(String Commands, Type toolsetType) {
            String[] cmds = CmdStringToArray(Commands);
            var method = toolsetType.GetMethods().Where(m=>m.Name == cmds[0]).FirstOrDefault();
            if (null == method) {
                Console.WriteLine("Command not found");
                return;
            }
            var args = cmds.Skip(1).ToArray();
            method?.Invoke(null, args);
        }

        static void MainLoop() {
            while (true) {
                Type toolsetType = typeof(Toolset);

                try {
                    new Thread(() => {
                        for (int i = 0; i < 2; i++) {
                            Console.Beep(1000, 200);
                            Thread.Sleep(TimeSpan.FromMilliseconds(20));
                        }
                    }).Start();
                    Console.Write("FTH> ");
                    String Commands = Console.ReadLine();
                    Exec(Commands, toolsetType);
                } catch (Exception x) {
                    var ex = x.InnerException;
                    int maxRec = 10;
                    while (ex != null && maxRec-- > 0) {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                        ex = ex.InnerException;
                    }
                }
            }
        }

        internal static String[] CmdStringToArray(String inputStr) {
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
    }
}