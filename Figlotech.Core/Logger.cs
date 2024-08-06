using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class Logger : ITextToFileLogger {
        public static int gid = 0;
        private int myid = ++gid;

        private int writec = 0;

        public bool Enabled { get; set; } = true;
        public IFileSystem FileAccessor { get; set; }

        public List<String> logLinesCache = new List<String>();
        private object BDLogLock = new Object();

        public bool EnableConsoleLogging { get; set; } = true;

        public FnVal<String> Filename { get; set; } = FnVal.From(() => DateTime.UtcNow.ToString("yyyy-MM-dd") + $" {Environment.MachineName}.txt");

        public Logger() { }
        public Logger(IFileSystem Accessor) {
            FileAccessor = Accessor;
        }

        public void ConversionLog(String log) {
            if (!Enabled)
                return;
            try {
                lock (String.Intern(Filename)) {
                    FileAccessor.AppendAllLines(
                        Filename, new String[] {
                            DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss - ") + log
                        });
                }
            } catch (Exception) { }

        }


        int maxBounce = 5;

        public void WriteLog(String log) {
            if (!Enabled)
                return;
            //Debug.WriteLine(log);
            Fi.Tech.FireAndForget(async () => {
                await Task.Yield();
                if (EnableConsoleLogging)
                    Console.Error.WriteLine(log);
                List<String> Lines = new List<String>();
                lock (this) {
                    log = Regex.Replace(log, @"\s+", " ");
                    String line = DateTime.Now.ToString("HH:mm:ss - ") + log;
                    Lines.AddRange(logLinesCache);
                    Lines.Add(line);
                    logLinesCache.Clear();
                }
                var errTreshold = 10;
                while(errTreshold --> 0) {
                    try {
                        lock (String.Intern(Filename.Value)) {
                            FileAccessor.AppendAllLines(
                                Filename.Value, Lines
                            );
                        }
                        break;
                    } catch (Exception x) {
                        await Task.Delay(1000);
                    }
                }
            });
        }

        public void BDadosLogDropLines() {
            if (!Enabled)
                return;
            try {
                lock (String.Intern(Filename)) {
                    FileAccessor.AppendAllLines(
                        Filename,
                        new String[] {
                            "","",""
                        });
                }
            } catch (Exception) { }
        }

        private void writeExInternal(string message, Exception x, StringBuilder sw = null) {
            bool isRoot = sw == null;
            sw = sw ?? new StringBuilder();
            if(isRoot) {
                sw.AppendLine($"-> {{");
                sw.AppendLine($" -- [{message}] -- {{");
            }
            sw.AppendLine($"[{x.Source}]--[{x.TargetSite}]--[{x.Message}]");
            sw.AppendLine(x.StackTrace);
            sw.AppendLine(new String('-', 20));
            if(x.InnerException != null) {
                writeExInternal(message, x.InnerException, sw);
            }
            if(x is AggregateException ag) {
                foreach(var agex in ag.InnerExceptions) {
                    writeExInternal(message, agex, sw);
                }
            }
            if(isRoot) {
                sw.AppendLine($"}} // {message} ");
                WriteLog(sw.ToString());
            }
        }

        public void WriteEx(String message, Exception x) {
            WriteLog(message);
            WriteLog(JsonConvert.SerializeObject(x.ToExceptionArray(), Formatting.Indented));
        }

        public void WriteLog(Exception x) {
            if (!Enabled || x == null)
                return;

            WriteLog($"[{x.Source}]--[{x.TargetSite}]--[{x.Message}]");
            WriteLog(x.StackTrace);
            WriteLog(new String('-', 20));
            if (x.InnerException != null) {
                WriteLog(x.InnerException);
            }
            if (x is AggregateException ag) {
                foreach (var agex in ag.InnerExceptions) {
                    WriteLog(agex);
                }
            }
        }

        public void SetLocation(string location) {

        }

        public void SetEnabled(bool enabled) {
            Enabled = enabled;
        }
    }
}
