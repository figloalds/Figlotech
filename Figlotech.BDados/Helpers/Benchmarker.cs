using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers {

    public class Benchmarker {
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

        public bool WriteToOutput { get; set; } = true;

        public Benchmarker(String name) {
            myName = name;
            marks.Add(new TimeMark(myName));
        }

        public double Mark(String txt) {
            marks.Add(new TimeMark(txt));
            var retv = marks[marks.Count - 1]
                .Timestamp.Subtract(marks[marks.Count - 2].Timestamp).TotalMilliseconds;
            var name = marks[marks.Count - 2].Name;
            //if (log) {
            //    Console.WriteLine($"{name}: {retv}ms");
            //}
            return retv;
        }

        public double TotalMark() {
            var mark = DateTime.UtcNow;
            marks.Add(new TimeMark(myName));
            var retv = marks[marks.Count - 1].Timestamp
                .Subtract(marks[0].Timestamp).TotalMilliseconds;
            if (WriteToOutput) {
                Console.WriteLine($"-- PERFORMANCE -----------");
                Console.WriteLine($"{myName}");
                Console.WriteLine($"--------------------------");
                for(int i = 1; i < marks.Count; i++) {
                    var time = marks[i]
                        .Timestamp.Subtract(marks[i - 1].Timestamp).TotalMilliseconds;
                    var name = marks[i - 1].Name;
                    Console.WriteLine($"{name}: {time}ms");
                }
                Console.WriteLine($"--------------------------");
                Console.WriteLine($"Total: {retv}ms");
            }
            return retv;
        }
    }
}
