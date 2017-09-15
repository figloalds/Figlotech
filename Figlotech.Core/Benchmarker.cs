using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;

namespace Figlotech.Core.Helpers {

    public class Benchmarker : IBenchmarker {
        private class TimeMark {
            public DateTime Timestamp;
            public String Name;

            public TimeMark(String txt) {
                Timestamp = DateTime.UtcNow;
                Name = txt;
            }
        }
        String myName;
        List<TimeMark> marks = new List<TimeMark>();

        public Benchmarker(String name) {
            myName = name;
            marks.Add(new TimeMark(myName));
        }

        public bool WriteToStdout { get; set; } = true;
        public bool Active { get; set; } = true;

        public double Mark(String txt) {
            if (!Active)
                return 0;
            marks.Add(new TimeMark(txt));
            var retv = marks[marks.Count - 1]
                .Timestamp.Subtract(marks[marks.Count - 2].Timestamp).TotalMilliseconds;
            var name = marks[marks.Count - 2].Name;
            //if (log) {
            //    Fi.Tech.WriteLine($"{name}: {retv}ms");
            //}
            return retv;
        }

        public double TotalMark() {
            if (!Active)
                return 0;
            var mark = DateTime.UtcNow;
            marks.Add(new TimeMark(myName));
            var retv = marks[marks.Count - 1].Timestamp
                .Subtract(marks[0].Timestamp).TotalMilliseconds;
            if (WriteToStdout) {
                Fi.Tech.WriteLine($"-- PERFORMANCE -----------");
                Fi.Tech.WriteLine($"{myName}");
                Fi.Tech.WriteLine($"--------------------------");
                double line = 0;
                Fi.Tech.WriteLine($" | 0ms ");
                for (int i = 1; i < marks.Count - 1; i++) {
                    var time = marks[i]
                        .Timestamp.Subtract(marks[i - 1].Timestamp).TotalMilliseconds;
                    var time2 = marks[i + 1]
                        .Timestamp.Subtract(marks[i].Timestamp).TotalMilliseconds;
                    line += time;
                    var name = marks[i - 1].Name;
                    Fi.Tech.WriteLine($" | {line}ms -> {name} ({time2}ms)");
                }
                Fi.Tech.WriteLine($" v {retv}ms");
                Fi.Tech.WriteLine($"--------------------------");
                Fi.Tech.WriteLine($"Total: {retv}ms");
            }
            return retv;
        }
    }
}