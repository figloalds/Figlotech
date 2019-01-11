using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace System
{
    public static class IEnumerableExtensions
    {

        public static DataTable ToDataTable<T>(this IEnumerable<T> me) {
            var dt = new DataTable();
            var enny = me.GetEnumerator();
            if (!enny.MoveNext())
                return dt;
            var refl = enny.Current.AsReflectable();
            foreach (var col in refl) {
                dt.Columns.Add(col.Key.Name);
            }
            Tuple<List<MemberInfo>, List<DataColumn>> meta = Fi.Tech.MapMeta(typeof(T), dt);
            do {
                var dr = dt.NewRow();
                enny.Current.ValuesToDataRow(dr, meta);
            } while (enny.MoveNext());
            return dt;
        }

        public static void ForEach<T>(this IEnumerable<T> me, Action<T> act) {
            var enumerator = me.GetEnumerator();
            while (enumerator.MoveNext()) {
                if (enumerator.Current != null) {
                    act?.Invoke(enumerator.Current);
                }
            }
        }

        public static IDictionary<TKey, TValue> ToIndex<TKey, TValue>(this IEnumerable<TValue> self, Func<TValue, TKey> keyFn) {
            Dictionary<TKey, TValue> retv = new Dictionary<TKey, TValue>();
            foreach(var v in self) {
                var k = keyFn(v);
                if(k != null && !retv.ContainsKey(k)) {
                    retv.Add(k, v);
                }
            }

            return retv;
        }

        public static void MoveAllTo<T>(this List<T> li, List<T> other, Predicate<T> predicate = null) {
            if (predicate == null) {
                predicate = (item) => true;
            }
            other.AddRange(li.Splice(predicate));
        }
                                                        // Cus I'm T int T, I'm dynamite.
        public static void IterateAssign<T>(this T[] me, Func<T, int, T> act) {
            me.ForEachIndexed((v, idx) => {
                me[idx] = act(v, idx);
            });
        }
        
        public static void ForEachReverse<T>(this IEnumerable<T> me, Action<T> act) {
            var stack = new Stack<T>();
            foreach(var item in me) {
                stack.Push(item);
            }

            while(stack.Count > 0) {
                act?.Invoke(stack.Pop());
            }
        }

        public static void ForEachIndexed<T>(this IEnumerable<T> me, Action<T, int> action) {
            int i = 0;
            foreach(var a in me) {
                action(a, i++);
            }
        }

        public static IEnumerable<T> ToColumn<T>(this T[,] me, int index) {
            for (int i = 0; i < me.GetLength(0); i++) {
                yield return me[i, index];
            }
        }
        public static IEnumerable<T> ToRow<T>(this T[,] me, int index) {
            for (int i = 0; i < me.GetLength(1); i++) {
                yield return me[index, i];
            }
        }

        public static IEnumerable<int> ToRange<T>(this IEnumerable<T> me) {
            int i = -1;
            foreach(var a in me) {
                yield return ++i;
            }
            yield break;
        }

        public static void EnqueueRange<T>(this Queue<T> me, IEnumerable<T> range) {
            range.ForEach(item => me.Enqueue(item));
        }
        public static IEnumerable<T> DequeueRangeAsLongAs<T>(this Queue<T> me, Predicate<T> condition) {
            T next;
            do {
                if (me.Count > 0) {
                    next = me.Peek();
                    if (condition(next)) {
                        yield return next;
                        continue;
                    }
                }
                yield break;
            } while (true);
        }

        public static IEnumerable<T> Slice<T>(this IEnumerable<T> t, int start, int end) {
            return t.Skip(start).Take(end);
        }
        public static IEnumerable<T> Flatten<T>(this IEnumerable<IEnumerable<T>> t) {
            foreach(var a in t) {
                foreach(var b in a) {
                    yield return b;
                }
            }
        }

        public static IEnumerable<IEnumerable<T>> Fracture<T>(this IEnumerable<T> t, int max) {
            List<T> li = new List<T>();
            foreach(var a in t) {
                li.Add(a);
                if (li.Count == max) {
                    yield return li.ToArray();
                    li.Clear();
                }
            }
            if (li.Count > 0) {
                yield return li.ToArray();
                li.Clear();
            }
        }

        public static IEnumerable<T> Combine<T>(this IEnumerable<T> t, IEnumerable<T> other) {
            foreach (var a in t) {
                yield return a;
            }
            foreach (var a in other) {
                yield return a;
            }
        }
        public static IEnumerable<T> Combine<T>(this IEnumerable<T> t, T other) {
            foreach (var a in t) {
                yield return a;
            }
            yield return other;
        }

        public static T MinBy<T, A>(this IEnumerable<T> me, Func<T, A> fn) where A : IComparable {
            var min = default(T);
            var minVal = default(A);
            var enumerator = me.GetEnumerator();
            int i = 0;
            while (enumerator.MoveNext()) {
                var val = fn(enumerator.Current);
                if (val.CompareTo(minVal) < 0) {
                    minVal = val;
                    min = enumerator.Current;
                }
            }
            return min;
        }

        public static T MaxBy<T, A>(this IEnumerable<T> me, Func<T, A> fn) where A : IComparable {
            var max = default(T);
            var maxVal = default(A);
            var enumerator = me.GetEnumerator();
            int i = 0;
            while (enumerator.MoveNext()) {
                var val = fn(enumerator.Current);
                if (val.CompareTo(maxVal) > 0) {
                    maxVal = val;
                    max = enumerator.Current;
                }
            }
            return max;
        }

        public static void ParallelForEach<T>(this IEnumerable<T> list, Action<T> work, Action<Exception> perWorkExceptionHandler = null, Action preWork = null, Action postWork = null, Action<Exception> preWorkExceptionHandling = null, Action<Exception> postWorkExceptionHandling = null) {
            var tempLi = list.ToList();
            Fi.Tech.BackgroundProcessList(tempLi, work, perWorkExceptionHandler, preWork, postWork, preWorkExceptionHandling, postWorkExceptionHandling);
        }

        public static IEnumerable<T> Splice<T>(this List<T> li, Predicate<T> predicate) {
            for(int i = li.Count-1; i >= 0; i--) {
                if(predicate(li[i])) {
                    var retv = li[i];
                    li.RemoveAt(i);
                    yield return retv;
                }
            }
        }

        public static T FirstOrDefaultBefore<T>(this IEnumerable<T> me, Predicate<T> predicate) {
            var enumerator = me.GetEnumerator();
            var retv = default(T);
            while (enumerator.MoveNext()) {
                if (predicate(enumerator.Current))
                    return retv;
                retv = enumerator.Current;
            }
            return default(T);
        }
        public static T FirstOrDefaultAfter<T>(this IEnumerable<T> me, Predicate<T> predicate) {
            var enumerator = me.GetEnumerator();
            while (enumerator.MoveNext()) {
                if (predicate(enumerator.Current)) {
                    if(enumerator.MoveNext()) {
                        return enumerator.Current;
                    }
                }
            }
            return default(T);
        }
    }
}
