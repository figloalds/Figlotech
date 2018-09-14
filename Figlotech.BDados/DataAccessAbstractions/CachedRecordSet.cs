using Figlotech.BDados.Builders;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class CachedRecordSet<T> : RecordSet<T> where T: IDataObject, new() {

        public CachedRecordSet() {

        }
        public CachedRecordSet(IDataAccessor da) :base(da) {
        }
        public bool AutoPersist { get; set; } = false;
        public CachedRecordSet(IDataAccessor dataAccessor, bool autoPersist = false) : base(dataAccessor) {
            AutoPersist = autoPersist;
        }

        public T GetItemOrDefault(Expression<Func<T, bool>> expr, Func<T> customInit = null) {

            var retv = this.FirstOrDefault(expr.Compile());
            if (retv == null && customInit != null) {
                retv = customInit.Invoke();
                if(retv != null) {
                    this.Add(retv);
                }
            }

            return retv;
        }

        public T GetItemOffline(Expression<Func<T, bool>> expr, Action<T> customInit = null) {

            var retv = this.FirstOrDefault(expr.Compile());
            if (retv == null && customInit != null) {
                retv = new T();
                customInit?.Invoke(retv);
                if (retv != null) {
                    this.Add(retv);
                }
            }

            return retv;
        }

        public T GetItemOrNull(Expression<Func<T, bool>> expr, Action<T> customInit = null) {
            var retv = this.FirstOrDefault(expr.Compile());
            if (retv == null) {
                retv = new RecordSet<T>(DataAccessor).LoadAll(new Conditions<T>(expr)).FirstOrDefault();
                if (retv != null) {
                    lock (listLockObject)
                        this.Add(retv);
                } else {
                    return default(T);
                }
            }

            return retv;
        }

        public T GetItem(Expression<Func<T, bool>> expr, Func<T> customInit = null) {
            var retv = this.FirstOrDefault(expr.Compile());
            if (retv == null) {
                retv = new RecordSet<T>(DataAccessor).LoadAll(new Conditions<T>(expr)).FirstOrDefault();
                if (retv != null) {
                    lock (listLockObject)
                        this.Add(retv);
                } else {
                    if (customInit != null) {
                        retv = customInit.Invoke();
                    } else {
                        retv = new T();
                    }
                    if (AutoPersist)
                        DataAccessor.SaveItem(retv);
                    //lock(listLockObject)
                    this.Add(retv);
                }
            }

            return retv;
        }

        private object listLockObject = new object();
    }
}
