using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;

namespace System {
    public static class BenchMarkerExtensions {
        public static double Mark(this Benchmarker self, String txt) {
            if (!FiTechCoreExtensions.EnableBenchMarkers)
                return 0;
            if (self == null) return 0;
            if (!self.Active)
                return 0;
            try {
                if(self.marks.Count > 1)
                    self.marks[self.marks.Count - 1].SetDuration(DateTime.UtcNow);
                self.marks.Add(new TimeMark(self.marks[0].Timestamp, txt));
                if (self.marks.Count < 2)
                    return 0;
                var retv = self.marks[self.marks.Count - 1]
                    .Timestamp.Subtract(self.marks[self.marks.Count - 2].Timestamp).TotalMilliseconds;

                return retv;
            } catch (Exception) {
            }
            return 0;
        }

        public static double FinalMark(this Benchmarker self) {
            if (!FiTechCoreExtensions.EnableBenchMarkers)
                return 0;
            if (self == null) return 0;
            if (!self.Active)
                return 0;
            try {
                if (self.marks.Count < 2)
                    self.marks.Add(new TimeMark(self.marks[0].Timestamp, self.myName));
                self.marks[self.marks.Count - 1].SetDuration(DateTime.UtcNow);
                var retv = self.marks[self.marks.Count - 1].Timestamp
                    .Subtract(self.marks[0].Timestamp).TotalMilliseconds;
                if (self.WriteToStdout) {
                    lock ("BENCHMARKER BM_WRITE") {
                        Console.WriteLine($"-- PERFORMANCE -----------");
                        Console.WriteLine($"{self.myName}");
                        Console.WriteLine($"--------------------------");
                        double line = 0;
                        Console.WriteLine($" | 0ms ");
                        for (int i = 0; i < self.marks.Count - 1; i++) {
                            Console.WriteLine($" | {self.marks[i].Name} +[{self.marks[i].Duration.TotalMilliseconds}ms]");
                        }
                        Console.WriteLine($" v {retv}ms");
                        Console.WriteLine($"--------------------------");
                        Console.WriteLine($"Total: {retv}ms");
                    }
                }
                return retv;
            } catch (Exception) { }
            return 0;
        }
    }

    internal class TimeMark {
        public DateTime Timestamp;
        public TimeSpan Duration;
        public String Name;

        public TimeMark(DateTime start, String txt) {
            Timestamp = DateTime.UtcNow;
            Name = $"{Timestamp.Subtract(start).TotalMilliseconds.ToString("0.0").PadLeft(7, ' ')}ms -->> {txt} ";
        }
        public void SetDuration(DateTime next) {
            Duration = next.Subtract(Timestamp);
        }
    }

    public class Benchmarker {
        internal String myName;
        internal List<TimeMark> marks = new List<TimeMark>();

        public Benchmarker(String name = "") {
            if (!FiTechCoreExtensions.EnableBenchMarkers)
                return;
            if (String.IsNullOrEmpty(name))
                name = new RID().AsBase36;
            myName = name;
            marks.Add(new TimeMark(DateTime.UtcNow, myName));
        }

        public bool WriteToStdout { get; set; } = true;
        public bool Active { get; set; } = true;
    }
}