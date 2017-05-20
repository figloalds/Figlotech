using Figlotech.BDados.Attributes;
using Figlotech.BDados.Builders;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.Requirements;
using Figlotech.Core;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Management;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity
{
    public abstract class BaseDataObject : 
        IDataObject, ISaveable, IBusinessObject, IAggregateRoot {

        public BaseDataObject(IDataAccessor dataAccessor, IContextProvider ctxProvider) {
            DataAccessor = dataAccessor;
            ContextProvider = ctxProvider;
        }

        public BaseDataObject() {
            DS.Default.SmartResolve(this);
        }

        public virtual long Id { get; set; }

        public virtual DateTime? UpdatedTime { get; set; }
        public virtual DateTime CreatedTime { get; set; }
        public virtual bool IsActive { get; set; }

        public virtual RID RID { get; set; }

        public abstract bool IsPersisted();

        public static Random r = new Random();

        protected IAggregateRoot OldValue { get; set; }

        private IDataAccessor dataAccessor;
        [JsonIgnore]
        public IDataAccessor DataAccessor { get; set; }

        [JsonIgnore]
        public IContextProvider ContextProvider { get; set; }

        public void ForceId(long newId) {
            this.Id = newId;
        }

        public void ForceRID(RID newRid) {
            RID = newRid;
        }

        public void Delete(ConditionParametrizer conditions = null)
        {
            DataAccessor.Delete(this);
        }

        public virtual bool Save(Action fn = null)
        {
            return DataAccessor.SaveItem(this);
        }
        
        public virtual bool Load(Action fn = null) {
            return false;
        }

        void CascadingDoForFields<T>(Action<T> process, List<Type> prevList = null) {
            if (prevList == null)
                prevList = new List<Type>();
            // prevList is a resource I'm using to break the fuck out of
            // looping references.
            // Idk if its the best way to, but it works.
            if (prevList.Contains(this.GetType()))
                return;
            prevList.Add(this.GetType());
            if (!typeof(T).IsInterface)
                throw new BDadosException("CascadingDoForFields expects T to be an Interface type.");
            var myValues = new List<MemberInfo>();
            myValues.AddRange(this.GetType().GetFields());
            myValues.AddRange(this.GetType().GetProperties());

            foreach (var field in myValues) {
                var workingValue = ReflectionTool.GetMemberValue(field, this);
                if (workingValue is BaseDataObject) {
                    ((BaseDataObject)workingValue).CascadingDoForFields(process, prevList);
                }
                if (!ReflectionTool.GetTypeOf(field).GetInterfaces().Contains(typeof(T)))
                    continue;
                prevList.Add(ReflectionTool.GetTypeOf(field));
                try {
                    T workObject = ((T)workingValue);
                    process(workObject);
                } catch (Exception) { }
            }
        }
        
        public virtual bool CascadingSave(Action fn = null, List<Type> alreadyTreatedTypes = null) {
            if(alreadyTreatedTypes == null) {
                alreadyTreatedTypes = new List<Type>();
            }
            if(alreadyTreatedTypes.Contains(this.GetType())) {
                return true;
            }
            alreadyTreatedTypes.Add(this.GetType());
            var errors = new ValidationErrors();

            if(this.RID == null) {
                this.Init();
            }

            errors.Merge(this.Validate());
            errors.Merge(this.ValidateInput());
            errors.Merge(this.ValidateBusiness());

            CascadingDoForFields<IBusinessObject>((field) => {
                errors.Merge(field?.ValidateInput());
                errors.Merge(field?.ValidateBusiness());
            });

            if (errors.Count > 0) {
                throw new ValidationException(errors);
            }

            if (null == DataAccessor) {
                throw new BDadosException($"Data Accessor is undefined in this instance of {this.GetType().Name}");
            }
            //if (this.GetType().BaseType != typeof(BaseDataObject)) {
            //    throw new BDadosException("BaseDataObject default IMPL doesn't support indirect aggregate save yet. Make sure this class imediately derives from BaseDataObject.");
            //}

            this.OnBeforePersist();

            var retv = true;

            //CascadingDoForFields<IAggregateRoot>((field) => {
            //    try {
            //        field.DataAccessor = DataAccessor;
            //        retv &= field?.CascadingSave(null, alreadyTreatedTypes) ?? true;
            //    }
            //    catch (Exception) { }
            //});

            retv &= DataAccessor.SaveItem(this);
            
            this.OnAfterPersist();
            
            return retv;
        }

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
