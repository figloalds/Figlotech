using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Attributes;
using Figlotech.BDados.Builders;
using System.Threading;

namespace Figlotech.BDados.Entity {

    internal interface IDataCache {
        Type Type { get; set; }
        DateTime LastUpdate { get; set; }
        List<IDataObject> GenericCache { get; }
    }

    internal class DataCache<T> : IDataCache where T: IDataObject, new() {
        public Type Type { get; set; }
        public DateTime LastUpdate { get; set; } = DateTime.MinValue;
        public List<IDataObject> GenericCache {
            get {
                var retv = new List<IDataObject>();
                foreach(var a in Cache) {
                    retv.Add((IDataObject)a);
                }
                return retv;
            }
        }
        public RecordSet<T> Cache { get; set; } = new RecordSet<T>();
        public DataCache() {
        }

    }

    /// <summary>
    /// I don't recommend using this.
    /// Seriously, dont.
    /// </summary>
    public class MemoryDataAccessor : IDataAccessor {

        private IDataCache GetCacheOf(Type t) {
            foreach (var a in CachesList) {
                if (a.Type == t) {
                    return a;
                }
            }
            var newDc = (IDataCache) Activator.CreateInstance(
                typeof(DataCache<>)
                .MakeGenericType(t));
            CachesList.Add(newDc);
            return newDc;
        }
        private DataCache<E> GetCacheOf<E>() where E : IDataObject, new() {
            foreach (var a in CachesList) {
                if (a.Type == typeof(E)) {
                    return (DataCache<E>)a;
                }
            }
            var newDc = new DataCache<E>();
            CachesList.Add(newDc);
            return newDc;
        }
        //private IDataAccessor DataAccessor;
        private List<IDataCache> CachesList = new List<IDataCache>();

        public MemoryDataAccessor() {
        }

        private ILogger l = new Logger();

        public ILogger Logger {
            get {
                return l;
            }

            set {
                l = value;
            }
        }

        public Type[] _workingTypes;
        public Type[] WorkingTypes {
            get { return _workingTypes; }
            set {
                _workingTypes = value.Where(t => t.GetInterfaces().Contains(typeof(IDataObject))).ToArray();
            }
        }

        private void AddCache<T>(RecordSet<T> recordSet) where T : IDataObject, new() {
            var thisCache = GetCacheOf<T>();
            lock(thisCache.Cache) {
                for (var i = 0; i < recordSet.Count; i++) {
                    bool found = false;
                    for (int c = 0; c < thisCache.Cache.Count; c++) {
                        if (thisCache.Cache[c].RID == recordSet[i].RID) {
                            found = true;
                            thisCache.Cache[c] = Clean(recordSet[i]);
                            break;
                        }
                    }
                    if (!found) {
                        thisCache.Cache.Add(Clean(recordSet[i]));
                    }
                }
            }
        }

        private T Clean<T>(T obj) where T : IDataObject, new() {
            var thisCache = GetCacheOf<T>();
            Type type = typeof(T);
            T retv = Activator.CreateInstance<T>();
            foreach (var a in type.GetFields()) {
                if (!a.GetCustomAttributes().Where(attr => attr.GetType() == typeof(FieldAttribute)).Any()) {
                    try {
                        ReflectionTool.SetValue(obj, a.Name, default(T));
                    } catch (Exception) { }
                }
            }
            return retv;
        }

        public RecordSet<T> CacheLoad<T>(Expression<Func<T, bool>> cnd) where T : IDataObject, new() {
            var thisCache = GetCacheOf<T>();
            var Cache = thisCache.Cache;
                var ret = new RecordSet<T>(this);
            if (cnd == null) {
                ret.AddRange(Cache);
            } else {
                ret.AddRange(Cache.Where(c => cnd.Compile()(c)));
            }
            return ret;
        }

        private T Decorate<T>(T input) where T : IDataObject, new() {
            var members = new List<MemberInfo>();
            members.AddRange(typeof(T).GetFields());
            members.AddRange(typeof(T).GetProperties());
            Dictionary<String, Object> References = new Dictionary<String, Object>();

            //Parallel.ForEach (members, (member) => {
            foreach (var member in members) {
                var aggField = member.GetCustomAttribute<AggregateFieldAttribute>();
                var aggObject = member.GetCustomAttribute<AggregateObjectAttribute>();
                var aggFarField = member.GetCustomAttribute<AggregateFarFieldAttribute>();
                var aggList = member.GetCustomAttribute<AggregateListAttribute>();

                if (aggField != null) {
                    if (References.ContainsKey(aggField.ObjectKey)) {
                        ReflectionTool.SetValue(input, member.Name, ReflectionTool.GetValue(References[aggField.ObjectKey], aggField.RemoteField));
                    } else {
                        var remoteType = aggField.RemoteObjectType;
                        var cache = GetCacheOf(aggField.RemoteObjectType);

                        var remote = cache?.GenericCache?.FirstOrDefault(
                                    (a) =>
                                        ReflectionTool.GetValue(input, aggField.ObjectKey)?.ToString() == a.RID?.ToString()
                                        || ReflectionTool.GetValue(a, aggField.ObjectKey)?.ToString() == input.RID?.ToString()
                                    );
                        ReflectionTool.SetValue(
                            input,
                            member.Name,
                            ReflectionTool.GetValue(
                                remote,
                                aggField.RemoteField
                            )
                        );
                        if (!References.ContainsKey(aggField.ObjectKey))
                            References.Add(aggField.ObjectKey, remote);
                    }
                }

                if (aggObject != null) {
                    var remoteType = aggField.RemoteObjectType;
                    var newObj = FetchByRid(remoteType, (string)ReflectionTool.GetValue(input, aggField.ObjectKey));
                }

                if (aggList != null) {
                    var remoteType = aggList.RemoteObjectType;
                    //dynamic newList = ReflectionTool.GetValue(input, member.Name);
                    var newList = GetAggList(remoteType, aggList.RemoteField, (string)input.RID);

                    ReflectionTool.SetValue(input, member.Name, Convert.ChangeType(newList, ReflectionTool.GetTypeOf(member)));
                }
            }
            //});
            return input;
        }
        private List<IDataObject> GetAggList(Type remoteType, string remoteField, string rid) {
            return 
                CachesList.Where((t) => t.Type.Name == remoteType.Name)
                    .FirstOrDefault()?
                    .GenericCache
                    .Where(
                        (f) => (string)ReflectionTool.GetValue(f, remoteField) == rid
                    ).ToList();
        }

        private object FetchByRid(Type remoteType, string v) {
            return CachesList.Where((c) => c.Type.Name == remoteType.Name)
                .FirstOrDefault()?
                .GenericCache.Where((o) => (string)o.RID == v).FirstOrDefault();
        }

        public RecordSet<T> AggregateLoad<T>(Expression<Func<T, bool>> cnd = null, int? limit = default(int?), int? page = default(int?), int PageSize = 200, MemberInfo OrderingMember = null, OrderingType Ordering = OrderingType.Asc, bool Linear = false) where T : IDataObject, new() {
            var cache = GetCacheOf<T>();
            RecordSet<T> retv = new RecordSet<T>(this);
            WorkQueuer.Live((queuer) => {
                foreach (var a in cache.Cache) {
                    var input = a;
                    queuer.Enqueue(() => {
                        var b = Decorate((T)input);
                        if (cnd == null || (cnd?.Compile()?.Invoke(b) ?? true)) {
                            retv.Add(b);
                        }
                    });
                }
            }, Environment.ProcessorCount);
            //RecordSet<T> retv = new RecordSet<T>();
            //Thread agl = new Thread(() => {
            //    retv.AddRange(DataAccessor.AggregateLoad<T>(cnd, limit, page, PageSize, OrderingMember, Ordering, Linear));
            //    AddCache<T>(retv);
            //});
            //agl.Name = $"AggregateLoad_{lc++}";
            //agl.Start();
            //if (cachedValues.Count == 0) {
            //    agl.Join();
            //    return retv;
            //}
            return retv;
        }

        public void Decorate<T>() where T : IDataObject, new() {
            var retv = GetCacheOf<T>().Cache;

            WorkQueuer.Live((queuer) => {
                for (int i = 0; i < retv.Count; i++) {
                    queuer.Enqueue(() => {
                        retv[i] = Decorate((T)retv[i]);
                    });
                }
            }, Environment.ProcessorCount);

        }

        public void Close() {
            //DataAccessor.Close();
        }

        public bool Delete<T>(T obj) where T : IDataObject, new() {
            var dl = GetCacheOf<T>();
            dl.Cache.Remove(obj);
            return true;
        }

        public bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new() {
            var dl = GetCacheOf<T>();
            var o = dl.Cache.Where((t) => condition.Compile().Invoke((T)t));
            foreach (var a in o) {
                dl.Cache.Remove(a);
            }
            return true;
        }

        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new() {
            var dl = GetCacheOf<T>();
            var o = dl.Cache.Where((t) => rids.Select((r) => r.RID).Contains(t.RID) && !cnd.Compile().Invoke((T)t));
            foreach (var a in o) {
                dl.Cache.Remove(a);
            }
            return true;
        }

        public T ForceExist<T>(Func<T> Default, Conditions<T> qb) where T : IDataObject, new() {
            var dl = GetCacheOf<T>();
            T o = (T)dl.Cache.Where((t) => ((Expression<Func<T, bool>>)qb.expression).Compile().Invoke((T)t)).FirstOrDefault();
            if (o == null) {
                o = Default.Invoke();
            }
            return o;
        }

        public T Instantiate<T>() where T : IDataObject, new() {
            return Activator.CreateInstance<T>();
        }

        static int lc = 0;
        public RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> cnd, int? page = default(int?), int? limit = 200) where T : IDataObject, new() {
            var cache = GetCacheOf<T>();
            //var cachedValues = CacheLoad<T>(cnd);]
            RecordSet<T> sel = new RecordSet<T>(this);
            var p = (page ?? 0) > 0 ? page ?? 1 : 1;
            var l = limit ?? int.MaxValue;
            if (cnd == null) {
                sel.AddRange(cache.Cache.Select(a => (T)a));
            } else {
                var offset = 0;
                if (limit != null) {
                    offset = (limit ?? int.MaxValue) * ((page ?? 1) - 1);
                }
                lock(cache.Cache) {
                    var fn = cnd.Compile();
                    RecordSet<T> retv = new RecordSet<T>(this);
                    foreach (var a in cache.Cache) {
                        var obj = (T)a;
                            if (fn(obj)) {
                                retv.Add(obj);
                            }
                    }
                    retv.AddRange(cache.Cache.Where(t => fn((T)t)).Select(a=>(T)a));

                    return retv;
                }
            }
            return sel;
        }

        public T LoadById<T>(long Id) where T : IDataObject, new() {
            var cachedValues = CacheLoad<T>((t) => t.Id == Id);
            return cachedValues.FirstOrDefault();
        }

        public T LoadByRid<T>(RID RID) where T : IDataObject, new() {
            if (RID == null)
                return default(T);
            var cachedValues = CacheLoad<T>((t) => t.RID == RID);
            return cachedValues.FirstOrDefault();
        }

        public IDataAccessor MakeNew() {
            return new MemoryDataAccessor();
        }

        public bool Open() {
            return true;
        }

        public bool SaveItem(IDataObject input, Action postSaveAction = null) {
            GetCacheOf(input.GetType()).GenericCache.Add(input);
            return true;
        }

        public bool Delete(IDataObject input) {
            GetCacheOf(input.GetType()).GenericCache.Remove(input);
            return true;
        }

        public bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new() {
            AddCache(rs);
            return true;
        }

        public T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = default(int?), int? limit = 200) where T : IDataObject, new() {
            return LoadAll<T>(condicoes, page, limit).FirstOrDefault();
        }
    }
}
