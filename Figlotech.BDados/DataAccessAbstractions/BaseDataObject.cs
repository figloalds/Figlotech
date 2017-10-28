using Figlotech.BDados.Builders;
using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Interfaces;
using Newtonsoft.Json;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {
    public abstract class BaseDataObject : IDataObject, ISaveable {

        public BaseDataObject(IDataAccessor dataAccessor, IContextProvider ctxProvider) {
            DataAccessor = dataAccessor;
            ContextProvider = ctxProvider;
        }

        public BaseDataObject() {
            DS.Default.SmartResolve(this, true);
        }

        public override string ToString() {

            return RID;
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
        
        // Optional stuff, override it or don't.
        // The save method will attempt to use them though.

        public virtual ValidationErrors Validate() { return new ValidationErrors();  }

        public virtual ValidationErrors ValidateInput() { return new ValidationErrors(); }

        public virtual ValidationErrors ValidateBusiness() { return new ValidationErrors(); }

        public virtual void SelfCompute(object Previous = null) { }

        public virtual void OnBeforePersist() { }

        public virtual void OnAfterPersist() { }

        public virtual void OnAfterLoad() { }

        public virtual void Init() { }
    }
}
