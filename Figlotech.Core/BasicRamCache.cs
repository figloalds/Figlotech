﻿using Figlotech.Core.Extensions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Figlotech.Core {
    public sealed class BasicMemoryCache {
        static int idGen = 0;
        int myId = ++idGen;

        private readonly ConcurrentDictionary<Type, object> DataCache = new ConcurrentDictionary<Type, object>();
        private readonly ConcurrentDictionary<Type, object> DataCacheByIndex = new ConcurrentDictionary<Type, object>();

        IDataAccessor DataAccessor { get; set; }

        public BasicMemoryCache(IDataAccessor dataAccessor = null) {
            DataAccessor = dataAccessor;
        }
        
        public ConcurrentDictionary<string, T> InternalCache<T>() where T : IDataObject, new() {
            return (ConcurrentDictionary<string, T>)DataCache.GetOrAddWithLocking(typeof(T), t => {
                var list = LoadListOfType<T>();
                return new ConcurrentDictionary<string, T>(list.ToDictionary(x => x.RID, x => x));
            });
        }

        public void Put<T>(IEnumerable<T> objs) where T : IDataObject, new() {
            foreach (var obj in objs) {
                Put(obj);
            }
        }

        public void Put<T>(T obj) where T : IDataObject, new() {
            var mainCache = InternalCache<T>();
            mainCache.AddOrUpdate(obj.RID, obj, (key, old) => obj);

            // If any index caches exist for T, update them.
            if (DataCacheByIndex.TryGetValue(typeof(T), out var indexCacheObj)
                && indexCacheObj is ConcurrentDictionary<string, ConcurrentDictionary<object, ConcurrentDictionary<string, T>>> indexCache) {
                foreach (var kv in indexCache) {
                    string indexName = kv.Key;
                    var indexDict = kv.Value; // Maps index value -> objects keyed by RID.
                    var idxValue = ReflectionTool.GetValue(obj, indexName);
                    var subDict = indexDict.GetOrAddWithLocking(idxValue, _ => new ConcurrentDictionary<string, T>());
                    subDict.AddOrUpdate(obj.RID, obj, (key, old) => obj);
                }
            }
        }

        public List<T> Get<T>() where T : IDataObject, new() {
            return InternalCache<T>().Values.ToList();
        }

        public T Find<T>(Func<T, bool> predicate) where T : IDataObject, new() {
            return InternalCache<T>().Values.FirstOrDefault(predicate);
        }

        public List<T> GetByIndex<T, TIndex>(Expression<Func<T, TIndex>> expr, TIndex indexValue) where T : IDataObject, new() {
            if (expr.Body is MemberExpression mex) {
                string indexName = mex.Member.Name;

                if (indexName == nameof(IDataObject.RID)) {
                    var mainCache = InternalCache<T>();
                    if (mainCache.TryGetValue(indexValue as String, out var obj)) {
                        return new List<T> { obj };
                    } else {
                        return new List<T>();
                    }
                }

                var typedIndexCache =
                    (ConcurrentDictionary<string, ConcurrentDictionary<object, ConcurrentDictionary<string, T>>>)
                    DataCacheByIndex.GetOrAddWithLocking(typeof(T), t =>
                        new ConcurrentDictionary<string, ConcurrentDictionary<object, ConcurrentDictionary<string, T>>>());

                var indexDict = typedIndexCache.GetOrAddWithLocking(indexName, name => {
                    var mainCache = InternalCache<T>();
                    var groups = mainCache.Values.GroupBy(x => ReflectionTool.GetValue(x, name));
                    var newIndexDict = new ConcurrentDictionary<object, ConcurrentDictionary<string, T>>();
                    foreach (var group in groups) {
                        newIndexDict.TryAdd(group.Key, new ConcurrentDictionary<string, T>(
                            group.ToDictionary(x => x.RID, x => x)
                        ));
                    }
                    return newIndexDict;
                });

                if (indexDict.TryGetValue(indexValue, out var subDict)) {
                    return subDict.Values.ToList();
                } else {
                    indexDict.TryAdd(indexValue, new ConcurrentDictionary<string, T>());
                    return new List<T>();
                }
            }

            throw new ArgumentException("Expression must be a MemberExpression");
        }

        public List<T> LoadListOfType<T>() where T : IDataObject, new() {
            return LoadAll.From<T>().Using(DataAccessor).Load();
        }
    }
}
