using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class CacheableDataAccessor : IDataAccessor {
        private IDataAccessor DataAccessor { get; set; }
        bool CacheEverything { get; set; }
        private SelfInitializerDictionary<Type, LenientDictionary<String, IDataObject>> Cache { get; set; } = new SelfInitializerDictionary<Type, LenientDictionary<string, IDataObject>>((type)=> new LenientDictionary<string, IDataObject>());
        private List<Type> CacheableTypes { get; set; } = new List<Type>();
        public CacheableDataAccessor(IDataAccessor da, bool cacheEverything) {
            throw new Exception("THIS IS A WORK IN PROGRESS");
            CacheEverything = true;
        }
        public CacheableDataAccessor(IDataAccessor da, Type[] TypesToCache) {
            throw new Exception("THIS IS A WORK IN PROGRESS");
            CacheableTypes.AddRange(TypesToCache);
        }

        public ILogger Logger { get => DataAccessor.Logger; set => DataAccessor.Logger = value; }
        public Type[] WorkingTypes { get => DataAccessor.WorkingTypes; set => DataAccessor.WorkingTypes = value; }

        public T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new() {
            if(CacheEverything || CacheableTypes.Contains(typeof(T))) {
                var retv = Cache[typeof(T)].Values.FirstOrDefault(e => (cnd.expression as Expression<Func<T, bool>>).Compile().Invoke((T) e));
                if (retv != null) {
                    return (T)retv;
                }
            }
            return DataAccessor.ForceExist(Default, cnd);
        }

        public List<T> LoadAll<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return DataAccessor.LoadAll(args);
        }

        public IEnumerable<T> Fetch<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return DataAccessor.Fetch(args);
        }

        public T LoadFirstOrDefault<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return DataAccessor.LoadFirstOrDefault(args);
        }

        public T LoadByRid<T>(string RID) where T : IDataObject, new() {
            return DataAccessor.LoadByRid<T>(RID);
        }

        public T LoadById<T>(long Id) where T : IDataObject, new() {
            return DataAccessor.LoadById<T>(Id);
        }

        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, List<T> rids) where T : IDataObject, new() {
            return DataAccessor.DeleteWhereRidNotIn(cnd, rids);
        }

        public bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new() {
            return DataAccessor.Delete(condition);
        }

        public bool Delete(IDataObject obj) {
            return DataAccessor.Delete(obj);
        }

        public bool SaveList<T>(List<T> rs, bool recoverIds = false) where T : IDataObject {
            return DataAccessor.SaveList(rs, recoverIds);
        }

        public bool SaveItem(IDataObject objeto) {
            return DataAccessor.SaveItem(objeto);
        }

        public bool Test() {
            return DataAccessor.Test();
        }

        public List<T> AggregateLoad<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return DataAccessor.AggregateLoad(args);
        }
    }
}
