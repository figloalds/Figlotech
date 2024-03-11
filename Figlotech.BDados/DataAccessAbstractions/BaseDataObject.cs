using Figlotech.BDados.Builders;
using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Interfaces;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public abstract class BaseDataObject : IDataObject {

        public BaseDataObject(IDataAccessor dataAccessor, IContextProvider ctxProvider) {
            DataAccessor = dataAccessor;
            ContextProvider = ctxProvider;
        }

        public BaseDataObject() {
        }

        public override string ToString() {

            return RID;
        }

        public abstract long Id { get; set; }
        public abstract DateTime? UpdatedTime { get; set; }
        public abstract DateTime CreatedTime { get; set; }
        public abstract bool IsActive { get; set; }
        public abstract String RID { get; set; }

        private bool? isPersisted = null;
        public bool IsPersisted {
            get {
                return isPersisted ?? (Id > 0);
            }
            set {
                isPersisted = value;
            }
        }

        [JsonIgnore]
        public virtual IDataAccessor DataAccessor { get; set; }

        [JsonIgnore]
        public IContextProvider ContextProvider { get; set; }

        public abstract ulong AlteredBy { get; set; }
        public abstract ulong CreatedBy { get; set; }

        public bool IsReceivedFromSync { get; set; }

        private int _persistedHash = 0;
        public int PersistedHash {
            get {
                if(!IsPersisted) {
                    return 0;
                }
                if (_persistedHash == 0) {
                    _persistedHash = this.SpFthComputeDataFieldsHash();
                }
                return _persistedHash;
            }
            set {
                _persistedHash = value;
            }
        }

        public void ForceId(long newId) {
            this.Id = newId;
        }

        public void ForceRID(String newRid) {
            RID = newRid;
        }

        public void Delete(ConditionParametrizer conditions = null) {
            DataAccessor.Delete(this);
        }

        // Optional stuff, override it or don't.
        // The save method will attempt to use them though.

        public virtual ValidationErrors Validate() { return new ValidationErrors(); }

        public virtual ValidationErrors ValidateInput() { return new ValidationErrors(); }

        public virtual ValidationErrors ValidateBusiness() { return new ValidationErrors(); }

        public virtual Task OnBeforePersistAsync() { return Task.CompletedTask;  }

        public virtual Task OnAfterPersistAsync() { return Task.CompletedTask; }

        public virtual void OnAfterLoad(DataLoadContext context) { }

        public virtual void SelfCompute(object Previous = null) { }

        public virtual void Init() { }
    }
}
