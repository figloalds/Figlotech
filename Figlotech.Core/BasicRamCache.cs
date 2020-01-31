using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.Core
{
    public class BasicMemoryCache
    {
        static int idGen = 0;
        int myId = ++idGen;
        private SelfInitializerDictionary<Type, object> DataCache = new SelfInitializerDictionary<Type, object>(
            t => {
                return Activator.CreateInstance(typeof(List<>).MakeGenericType(t));
            }
        );

        IDataAccessor DataAccessor { get; set; }
        public BasicMemoryCache(IDataAccessor dataAccessor = null) {
            DataAccessor = dataAccessor;
        }

        private List<T> InternalCache<T>() where T : IDataObject, new()  {
            lock (String.Intern($"{myId}_BasicMemoryCache_{typeof(T).Name}")) {
                if(!DataCache.ContainsKey(typeof(T))) {
                    DataCache[typeof(T)] = LoadListOfType<T>();
                }
                return ((List<T>)DataCache[typeof(T)]);
            }
        }

        public void Put<T>(IEnumerable<T> objs) where T : IDataObject, new() {
            lock (String.Intern($"{myId}_BasicMemoryCache_{typeof(T).Name}")) {
                foreach (var a in objs) {
                    Put(a);
                }
            }
        }

        public void Put<T>(T obj) where T : IDataObject, new() {
            lock (String.Intern($"{myId}_BasicMemoryCache_{typeof(T).Name}")) {
                var ic = InternalCache<T>();
                var idx = ic.FindIndex(x => x.RID == obj.RID);
                if (idx > -1) {
                    ic[idx] = obj;
                } else {
                    ic.Add(obj);
                }
            }
        }

        public List<T> Get<T>() where T : IDataObject, new() {
            lock (String.Intern($"{myId}_BasicMemoryCache_{typeof(T).Name}")) {
                
                return InternalCache<T>().ToList();
            }
        }

        public T Find<T>(Func<T, bool> predi) where T : IDataObject, new() {
            lock (String.Intern($"{myId}_BasicMemoryCache_{typeof(T).Name}")) {
                return Get<T>().FirstOrDefault(predi);
            }
        }

        public List<T> LoadListOfType<T>() where T: IDataObject, new() {
            return LoadAll.From<T>().Using(DataAccessor).Load();
        }
    }
}
