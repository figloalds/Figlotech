using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class DataObject<T> : BaseDataObject, IBusinessObject<T> where T : IDataObject, new() {

        [PrimaryKey]
        [Field(Options = "NOT NULL AUTO_INCREMENT PRIMARY KEY")]
        public override long Id { get; set; } = 0;

        [Field(AllowNull = true, DefaultValue = null)]
        public override DateTime? UpdatedTime { get; set; } = DateTime.UtcNow;

        [NoUpdate]
        [Field(Type = "TIMESTAMP", AllowNull = false, DefaultValue = "CURRENT_TIMESTAMP")]
        public override DateTime CreatedTime { get; set; } = DateTime.UtcNow;

        private Lazy<RID> _rid = new Lazy<RID>(() => new RID());
        [ReliableId]
        [Field(Size = 64, AllowNull = false, Unique = true)]
        public override String RID { get { return _rid.Value.ToString(); } set { _rid = new Lazy<RID>(() => value); } }

        public override bool IsPersisted { get { return Id > 0; } }

        public override bool IsActive { get; set; }



        public DataObject() { }
        public DataObject(IDataAccessor dataAccessor, IContextProvider ctxProvider) : base (dataAccessor, ctxProvider) {

        }

        public virtual List<IValidationRule<T>> ValidationRules { get; set; } = new List<IValidationRule<T>>();

        public override ValidationErrors Validate() {
            ValidationErrors ve = new ValidationErrors();
            var myType = this.GetType();

            var myValues = new List<MemberInfo>();
            myValues.AddRange(this.GetType().GetFields());
            myValues.AddRange(this.GetType().GetProperties());

            foreach(var a in ValidationRules) {
                foreach(var err in a.Validate(this, new ValidationErrors())) {
                    ve.Add(err);
                }
            }

            var reflector = new ObjectReflector(this);
            foreach (var field in myValues.Where((f) => f.GetCustomAttribute<ValidationAttribute>() != null)) {
                var info = field.GetCustomAttribute<ValidationAttribute>();
                foreach(var error in info.Validate(field, reflector[field])) {
                    ve.Add(error);
                }
            }

            // Validations
            foreach (var field in myValues.Where((f) => f.GetCustomAttribute<ValidationAttribute>() != null)) {
                var vAttribute = field.GetCustomAttribute<ValidationAttribute>();
                if (vAttribute != null) {
                    foreach(var a in vAttribute.Validate(field, ReflectionTool.GetMemberValue(field, this)))
                        ve.Add(a);
                }
            }

            return ve;
        }

        public override bool Save(Action fn = null) {
            return DataAccessor.SaveItem(this);
        }

        public override bool Load(Action fn = null) {
            if(this.Id > 0) {
                FTH.MemberwiseCopy(
                    DataAccessor.LoadByRid<T>(this.RID), this);
                return true;
            } else {
                return false;
            }
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
                if (workingValue is T) {
                    CascadingDoForFields<T>(process, prevList);
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
            if (alreadyTreatedTypes == null) {
                alreadyTreatedTypes = new List<Type>();
            }
            if (alreadyTreatedTypes.Contains(this.GetType())) {
                return true;
            }
            alreadyTreatedTypes.Add(this.GetType());
            var errors = new ValidationErrors();

            if (this.RID == null) {
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
            this.OnBeforePersist();

            var retv = true;

            retv &= DataAccessor.SaveItem(this);

            this.OnAfterPersist();

            return retv;
        }
    }
}
