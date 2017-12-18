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
                self.marks.Add(new TimeMark(txt));
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
                var mark = DateTime.UtcNow;
                self.marks.Add(new TimeMark(self.myName));
                var retv = self.marks[self.marks.Count - 1].Timestamp
                    .Subtract(self.marks[0].Timestamp).TotalMilliseconds;
                if (self.WriteToStdout) {
                    Console.WriteLine($"-- PERFORMANCE -----------");
                    Console.WriteLine($"{self.myName}");
                    Console.WriteLine($"--------------------------");
                    double line = 0;
                    Console.WriteLine($" | 0ms ");
                    for (int i = 1; i < self.marks.Count - 1; i++) {
                        try {
                            var time = self.marks[i]
                                .Timestamp.Subtract(self.marks[i - 1].Timestamp).TotalMilliseconds;
                            var time2 = self.marks[i + 1]
                                .Timestamp.Subtract(self.marks[i].Timestamp).TotalMilliseconds;
                            line += time;
                            var name = self.marks[i - 1].Name;
                            Console.WriteLine($" | {line}ms -> {name} ({time2}ms)");
                        } catch (Exception) {

                        }
                    }
                    Console.WriteLine($" v {retv}ms");
                    Console.WriteLine($"--------------------------");
                    Console.WriteLine($"Total: {retv}ms");
                }
                return retv;
            } catch (Exception) { }
            return 0;
        }
    }

    internal class TimeMark {
        public DateTime Timestamp;
        public String Name;

        public TimeMark(String txt) {
            Timestamp = DateTime.UtcNow;
            Name = txt;
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
            marks.Add(new TimeMark(myName));
        }

        public bool WriteToStdout { get; set; } = true;
        public bool Active { get; set; } = true;
    }
}