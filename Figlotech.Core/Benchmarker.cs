using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace System {


    public static class BenchMarkerExtensions {
        public static double Mark(this Benchmarker self, String txt, Exception ex = null) {
            if (!FiTechCoreExtensions.EnableBenchMarkers)
                return 0;
            if (self == null) return 0;
            if (!self.Active)
                return 0;
            try {
                if(self.marks.Count > 1)
                    self.marks[self.marks.Count - 1].SetDuration(DateTime.UtcNow);
                self.marks.Add(new TimeMark(self.marks[0].Timestamp, txt) {
                    Exception = ex
                });
                if (self.marks.Count < 2)
                    return 0;
                var retv = self.marks[self.marks.Count - 1]
                    .Timestamp.Subtract(self.marks[self.marks.Count - 2].Timestamp).TotalMilliseconds;

                return retv;
            } catch (Exception) {
            }
            return 0;
        }

        public static async Task<T> Wrap<T>(this Benchmarker self, string label, Func<Task<T>> action) {
            T retv;
            try {
                self.Mark($"[RegionStart] {label}");
                retv = await action.Invoke();
                self.Mark($"[RegionEndOk] {label}");
            }
            catch (Exception x) {
                self.Mark($"[RegionException] {label}", x);
                retv = default(T);
                throw x;
            }

            return retv;
        }

        public static T Wrap<T>(this Benchmarker self, string label, Func<T> action) {
            T retv;
            try {
                self.Mark($"[RegionStart] {label}");
                retv = action.Invoke();
                self.Mark($"[RegionEndOk] {label}");
            } catch(Exception x) {
                self.Mark($"[RegionException] {label}", x);
                retv = default(T);
                throw x;
            }

            return retv;
        }

        public static void Wrap(this Benchmarker self, string label, Action action) {
            try {
                self.Mark($"[RegionStart] {label}");
                action.Invoke();
                self.Mark($"[RegionEndOk] {label}");
            }
            catch (Exception x) {
                self.Mark($"[RegionException] {label}", x);
                throw x;
            }
        }

        public static void Assert(this Benchmarker self, Expression<Func<bool>> expr) {
            if (!FiTechCoreExtensions.EnableBenchMarkers ||  expr == null)
                return;
            try {
                bool result = expr.Compile().Invoke();
                if (!result && Debugger.IsAttached) {
                    self.Mark($"Assert Failed {expr.ToString()} => {result}");
                    Debugger.Break();
                }
            } catch(Exception x) {
                self.Mark($"Assert {expr.ToString()} => Exception", x);
                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
            }
        }

        public static IEnumerable<String> VerboseLog(this Benchmarker self) {
            if (!FiTechCoreExtensions.EnableBenchMarkers)
                yield break;
            if (self == null)
                yield break;

            var lines = new List<String>();

            var retv = (self.marks[self.marks.Count-1].Timestamp - self.marks[0].Timestamp).TotalMilliseconds;
            yield return ($"--------------------------");
            yield return ($"{self.myName}");
            yield return ($"--------------------------");
            yield return ($" | 0ms ");
            for (int i = 0; i < self.marks.Count - 1; i++) {
                yield return ($" | [{i}] {self.marks[i].Name} +[{self.marks[i].Duration.TotalMilliseconds}ms]");
                if (self.marks[i].Exception != null) {
                    var ex = self.marks[i].Exception;
                    do {
                        yield return ($" | [{i}] => Thrown {self.marks[i].Exception.Message}");
                        yield return ($" | [{i}] => StackTrace {self.marks[i].Exception.StackTrace}");
                        ex = ex.InnerException;
                    } while (ex != null);
                }
            }
            yield return ($" v {retv}ms");
            yield return ($"--------------------------");
            yield return ($"Total: {retv}ms");
        }

        public static double FinalMark(this Benchmarker self) {
            if (!FiTechCoreExtensions.EnableBenchMarkers)
                return 0;
            if (self == null) return 0;
            if (!self.Active)
                return 0;
            try {
                var initMark = self.marks[0];
                if (self.marks.Count < 2)
                    self.marks.Add(new TimeMark(initMark.Timestamp, self.myName));
                var lastMark = self.marks[self.marks.Count - 1];
                lastMark.SetDuration(DateTime.UtcNow);
                var retv = (self.marks[self.marks.Count - 1].Timestamp - self.marks[0].Timestamp).TotalMilliseconds;
                if (self.WriteToStdout) {
                    lock ("BENCHMARKER BM_WRITE") {
                        self.VerboseLog().ForEach(Console.WriteLine);
                    }
                }
                return retv;
            } catch (Exception) { }
            return 0;
        }
    }

    internal class TimeMark {
        public DateTime Timestamp { get; set; }
        public TimeSpan Duration { get; set; }
        public String Name { get; set; }
        public Exception Exception { get; set; }

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