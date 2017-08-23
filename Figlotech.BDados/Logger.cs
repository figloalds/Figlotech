using Figlotech.BDados.FileAcessAbstractions;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Figlotech.BDados {
    public class Logger : ILogger {
        public static int gid = 0;
        private int myid = ++gid;
        private int writec = 0;
        public bool Enabled { get; set; }
        public IFileAccessor FileAccessor { get; set; }

        public List<String> BDadosLogCache = new List<String>();
        private object BDLogLock = new Object();

        public bool EnableConsoleLogging { get; set; } = true;
        
        public Logger() { }
        public Logger(IFileAccessor Accessor) {
            FileAccessor = Accessor;
        }

        public void ConversionLog(String log) {
            if (!Enabled)
                return;
            try {
                FileAccessor.AppendAllLines(
                    "LogConverts.txt", new String[] {
                        DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss - ") + log
                    });
            } catch (Exception) { }

        }

        int maxBounce = 5;
        public void WriteLog(String log) {
            if (!Enabled)
                return;
            try {

                FTH.GlobalQueuer.Enqueue((p) => {
                    if (EnableConsoleLogging)
                        FTH.WriteLine(log);
                    log = Regex.Replace(log, @"\s+", " ");
                    lock ("BDADOS_LOG_LOCK") {
                        String line = DateTime.Now.ToString("HH:mm:ss - ") + log;
                        try {

                            List<String> Lines = new List<String>();
                            Lines.AddRange(BDadosLogCache);
                            Lines.Add(line);
                            FileAccessor.AppendAllLines(
                                DateTime.UtcNow.ToString("yyyy-MM-dd") + $" {Environment.MachineName}.txt", Lines);
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
                    DateTime.UtcNow.ToString("yyyy-MM-dd") + ".txt",
                    new String[] {
                        "","",""
                    });
            } catch (Exception) { }
        }

        public void WriteLog(Exception x) {
            if (!Enabled)
                return;
            try {

                FTH.GlobalQueuer.Enqueue((p) => {

                    lock ("BDADOS_LOG_LOCK") {

                        var y = x;
                        while (y != null) {
                            if (EnableConsoleLogging)
                                FTH.WriteLine(y.Message);
                            y = y.InnerException;
                        }
                        try {
                            List<String> trace = new List<String>();
                            StackTrace s = new StackTrace(x, true);
                            StackFrame[] Frames = s.GetFrames();
                            trace.Add(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + " ---- Exception ---------------------------------- ");
                            for (int i = 0; i < Frames.Length; i++) {
                                if (Frames[i].GetMethod().DeclaringType.Name.Contains("BDados") || Frames[i].GetMethod().DeclaringType.Name.Contains("MySql"))
                                    continue;
                                trace.Add(String.Format("-- {0}, Linha {1}", Frames[i].GetFileName(), Frames[i].GetFileLineNumber()));
                            }
                            trace.Add("--------------------------------------------------------------------- ");
                            FileAccessor.AppendAllLines(
                                DateTime.UtcNow.ToString("yyyy-MM-dd") + ".txt",
                                new String[] {
                                DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss ---- Exception ---------------------------------- "),
                                x.Message,
                                x.StackTrace,
                                DateTime.Now.ToString("--------------------------------------------------------------------- ")
                                });
                        } catch (Exception) { }

                    }
                });
            } catch (Exception) { }
        }

        public void SetLocation(string location) {

        }

        public void SetEnabled(bool enabled) {
            Enabled = enabled;
        }
    }
}
