using Figlotech.BDados.Builders;
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
        public CachedRecordSet(IDataAccessor dataAccessor) : base(dataAccessor) {

        }


        public T FetchSimple(Expression<Func<T, bool>> expr, Action<T> customInit = null) {

            var retv = this.FirstOrDefault(expr.Compile());
            if (retv == null) {
                retv = new T();
                customInit?.Invoke(retv);
            }

            return retv;
        }

        public T FetchOrNull(Expression<Func<T, bool>> expr, Action<T> customInit = null) {
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

        public T Fetch(Expression<Func<T, bool>> expr, Action<T> customInit = null) {
            var retv = this.FirstOrDefault(expr.Compile());
            if (retv == null) {
                retv = new RecordSet<T>(DataAccessor).LoadAll(new Conditions<T>(expr)).FirstOrDefault();
                if (retv != null) {
                    lock (listLockObject)
                        this.Add(retv);
                } else {
                    retv = new T();
                    customInit?.Invoke(retv);
                    lock(listLockObject)
                        this.Add(retv);
                }
            }

            return retv;
        }

        private object listLockObject = new object();
    }
}
