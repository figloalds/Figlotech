using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Extensions.AsyncEnumerable
{
    public static class IAsyncEnumerableExtensions
    {

        public static async IAsyncEnumerable<T> Skip<T>(this IAsyncEnumerable<T> arraySegment, int count) {
            int i = 0;
            await foreach(var item in arraySegment) {
                if(i++ < count) {
                    continue;
                }
                yield return item;
            }
        }

        public static async Task<int> Count<T>(this IAsyncEnumerable<T> arraySegment) {
            int i = 0;
            await foreach(var item in arraySegment) {
                i++;
            }
            return i;
        }

        public static async IAsyncEnumerable<T> Take<T>(this IAsyncEnumerable<T> arraySegment, int count) {
            int i = 0;
            await foreach(var item in arraySegment) {
                if(i++ >= count) {
                    break;
                }
                yield return item;
            }
        }

        public static async IAsyncEnumerable<T> Where<T>(this IAsyncEnumerable<T> arraySegment, Func<T, bool> predicate) {
            await foreach(var item in arraySegment) {
                if(predicate(item)) {
                    yield return item;
                }
            }
        }

        public static async IAsyncEnumerable<U> Select<T, U>(this IAsyncEnumerable<T> arraySegment, Func<T, U> selector) {
            await foreach(var item in arraySegment) {
                yield return selector(item);
            }
        }

        public static async IAsyncEnumerable<U> SelectMany<T, U>(this IAsyncEnumerable<T> arraySegment, Func<T, IAsyncEnumerable<U>> selector) {
            await foreach(var item in arraySegment) {
                await foreach(var subItem in selector(item)) {
                    yield return subItem;
                }
            }
        }

        public static async IAsyncEnumerable<T> Concat<T>(this IAsyncEnumerable<T> arraySegment, IAsyncEnumerable<T> other) {
            await foreach(var item in arraySegment) {
                yield return item;
            }
            await foreach(var item in other) {
                yield return item;
            }
        }

        public static async IAsyncEnumerable<T> Distinct<T>(this IAsyncEnumerable<T> arraySegment) {
            HashSet<T> set = new HashSet<T>();
            await foreach(var item in arraySegment) {
                if(set.Add(item)) {
                    yield return item;
                }
            }
        }

        public static async IAsyncEnumerable<T> Union<T>(this IAsyncEnumerable<T> arraySegment, IAsyncEnumerable<T> other) {
            HashSet<T> set = new HashSet<T>();
            await foreach(var item in arraySegment) {
                if(set.Add(item)) {
                    yield return item;
                }
            }
            await foreach(var item in other) {
                if(set.Add(item)) {
                    yield return item;
                }
            }
        }

        public static async IAsyncEnumerable<T> Intersect<T>(this IAsyncEnumerable<T> arraySegment, IAsyncEnumerable<T> other) {
            HashSet<T> set = new HashSet<T>();
            HashSet<T> set2 = new HashSet<T>();
            await foreach(var item in arraySegment) {
                set.Add(item);
            }
            await foreach(var item in other) {
                set2.Add(item);
            }
            foreach(var item in set) {
                if(set2.Contains(item)) {
                    yield return item;
                }
            }
        }

        public static async IAsyncEnumerable<T> Except<T>(this IAsyncEnumerable<T> arraySegment, IAsyncEnumerable<T> other) {
            HashSet<T> set = new HashSet<T>();
            HashSet<T> set2 = new HashSet<T>();
            await foreach(var item in arraySegment) {
                set.Add(item);
            }
            await foreach(var item in other) {
                set2.Add(item);
            }
            foreach(var item in set) {
                if(!set2.Contains(item)) {
                    yield return item;
                }
            }
        }

        public static async IAsyncEnumerable<T> OrderBy<T>(this IAsyncEnumerable<T> arraySegment, Func<T, IComparable> selector) {
            List<T> list = new List<T>();
            await foreach(var item in arraySegment) {
                list.Add(item);
            }
            list.Sort((a, b) => selector(a).CompareTo(selector(b)));
            foreach(var item in list) {
                yield return item;
            }
        }

        public static async IAsyncEnumerable<T> OrderByDescending<T>(this IAsyncEnumerable<T> arraySegment, Func<T, IComparable> selector) {
            List<T> list = new List<T>();
            await foreach(var item in arraySegment) {
                list.Add(item);
            }
            list.Sort((a, b) => selector(b).CompareTo(selector(a)));
            foreach(var item in list) {
                yield return item;
            }
        }
    }
}
