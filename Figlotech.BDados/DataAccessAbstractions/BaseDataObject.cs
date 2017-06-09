using Figlotech.BDados.Builders;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public abstract class BaseDataObject : 
        IDataObject, ISaveable, IBusinessObject {

        public BaseDataObject(IDataAccessor dataAccessor, IContextProvider ctxProvider) {
            DataAccessor = dataAccessor;
            ContextProvider = ctxProvider;
        }

        public BaseDataObject() {
            DS.Default.SmartResolve(this, true);
        }

        public abstract long Id { get; set; }
        public abstract DateTime? UpdatedTime { get; set; }
        public abstract DateTime CreatedTime { get; set; }
        public abstract bool IsActive { get; set; }
        public abstract String RID { get; set; }
        public abstract bool IsPersisted { get; }

        [JsonIgnore]
        public IDataAccessor DataAccessor { get; set; }

        [JsonIgnore]
        public IContextProvider ContextProvider { get; set; }

        public void ForceId(long newId) {
            this.Id = newId;
        }

        public void ForceRID(String newRid) {
            RID = newRid;
        }

        public void Delete(ConditionParametrizer conditions = null)
        {
            DataAccessor.Delete(this);
        }

        public abstract bool Save(Action fn = null);
        public abstract bool Load(Action fn = null);


        public virtual IAggregateRoot AggregateLoadById(long id) {
            throw new NotImplementedException();
        }

        public virtual IAggregateRoot AggregateLoadByRid(string rid) {
            throw new NotImplementedException();
        }

        // Optional stuff, override it or don't.
        // The save method will attempt to use them though.

        public virtual ValidationErrors Validate() { return new ValidationErrors();  }

        public virtual ValidationErrors ValidateInput() { return new ValidationErrors(); }

        public virtual ValidationErrors ValidateBusiness() { return new ValidationErrors(); }

        public virtual void SelfCompute(object Previous = null) { }

        public virtual void OnBeforePersist() { }

        public virtual void OnAfterPersist() { }

        public virtual void Init() { }
    }
}
