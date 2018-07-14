using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;

namespace Figlotech.Core {
    public class Logger : ILogger {
        public static int gid = 0;
        private int myid = ++gid;
        private int writec = 0;
        public bool Enabled { get; set; } = true;
        public IFileSystem FileAccessor { get; set; }

        public List<String> BDadosLogCache = new List<String>();
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
                FileAccessor.AppendAllLines(
                    Filename, new String[] {
                        DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss - ") + log
                    });
            } catch (Exception) { }

        }

        int maxBounce = 5;
        public void WriteLog(String log) {
            if (!Enabled)
                return;
            try {
                //Debug.WriteLine(log);
                Fi.Tech.RunAndForget(() => {
                    if (EnableConsoleLogging)
                        Console.Error.WriteLine(log);
                    log = Regex.Replace(log, @"\s+", " ");
                    lock ("BDADOS_LOG_LOCK") {
                        String line = DateTime.Now.ToString("HH:mm:ss - ") + log;
                        try {

                            List<String> Lines = new List<String>();
                            Lines.AddRange(BDadosLogCache);
                            Lines.Add(line);
                            FileAccessor.AppendAllLines(
                                Filename, Lines);
                            BDadosLogCache.Clear();
                        } catch (Exception) {
                        }
                    }
                });
            } catch (Exception) {
            }
        }

        public void BDadosLogDropLines() {
            if (!Enabled)
                return;
            try {
                FileAccessor.AppendAllLines(
                    Filename, 
                    new String[] {
                        "","",""
                    });
            } catch (Exception) { }
        }
        
        public void WriteEx(Exception x, StreamWriter sw) {
            sw.WriteLine($"[{x.Source}]--[{x.TargetSite}]--[{x.Message}]");
            sw.WriteLine(x.StackTrace);
            sw.WriteLine(new String('-', 20));
            if(x.InnerException != null) {
                WriteEx(x, sw);
            }
            if(x is AggregateException ag) {
                foreach(var agex in ag.InnerExceptions) {
                    WriteEx(agex, sw);
                }
            }
        }

        public void WriteLog(Exception x) {
            if (!Enabled)
                return;

            WriteLog($"[{x.Source}]--[{x.TargetSite}]--[{x.Message}]");
            WriteLog(x.StackTrace);
            WriteLog(new String('-', 20));
            if (x.InnerException != null) {
                WriteLog(x);
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
