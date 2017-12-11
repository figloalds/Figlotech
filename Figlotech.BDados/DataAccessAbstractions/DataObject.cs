using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.BusinessModel;

namespace Figlotech.BDados.DataAccessAbstractions {
    public abstract class DataObject<T> : BaseDataObject, IDataObject, IBusinessObject where T: IDataObject, IBusinessObject, new() {

        [PrimaryKey]
        [Field(Options = "NOT NULL AUTO_INCREMENT PRIMARY KEY")]
        public override long Id { get; set; } = 0;

        [Field(AllowNull = true, DefaultValue = null)]
        public override DateTime? UpdatedTime { get; set; } = DateTime.UtcNow;

        [NoUpdate]
        [Field(Type = "TIMESTAMP", AllowNull = false, DefaultValue = "CURRENT_TIMESTAMP")]
        public override DateTime CreatedTime { get; set; } = DateTime.UtcNow;

        private String _rid = null;

        [ReliableId]
        [Field(Size = 64, AllowNull = false, Unique = true)]
        public override String RID {
            get {
                if (_rid != null)
                    return _rid;
                return _rid = new RID().ToString();
            }
            set {
                _rid = value;
            }
        }

        public override bool IsActive { get; set; }

        public DataObject() { }

        protected abstract IEnumerable<IValidationRule<T>> ValidationRules();

        public override ValidationErrors Validate() {
            ValidationErrors ve = new ValidationErrors();
            var myType = this.GetType();

            var myValues = new List<MemberInfo>();
            myValues.AddRange(this.GetType().GetFields());
            myValues.AddRange(this.GetType().GetProperties());

            T obj = obj = (T)(IBusinessObject)this;

            foreach (var a in ValidationRules()) {
                foreach (var err in a.Validate(obj, new ValidationErrors())) {
                    ve.Add(err);
                }
            }

            // Validations
            foreach (var field in myValues.Where((f) => f.GetCustomAttribute<ValidationAttribute>() != null)) {
                var vAttribute = field.GetCustomAttribute<ValidationAttribute>();
                if (vAttribute != null) {
                    foreach (var a in vAttribute.Validate(field, ReflectionTool.GetMemberValue(field, this)))
                        ve.Add(a);
                }
            }

            return ve;
        }

        public override bool Save(Action fn = null) {
            return DataAccessor.SaveItem(this);
        }

        public override bool Load(Action fn = null) {
            if (this.Id > 0) {
                Fi.Tech.MemberwiseCopy(
                    DataAccessor.LoadByRid<T>(this.RID), this);
                return true;
            }
            else {
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
                }
                catch (Exception) { }
            }
        }
        InstanceAuthorizer ia = new InstanceAuthorizer();
        public string RunValidations() {

            var errors = new ValidationErrors();

            errors.Merge(this.Validate());
            errors.Merge(this.ValidateInput());
            errors.Merge(this.ValidateBusiness());

            CascadingDoForFields<IBusinessObject>((field) => {
                errors.Merge(field?.ValidateInput());
                errors.Merge(field?.ValidateBusiness());
            });

            if (errors.Count > 0) {
                throw new BusinessValidationException(errors);
            }

            return ia.GenerateAuthorization();
        }

        public virtual bool ValidateAndPersist(String iaToken) {
            if (!ia.CheckAuthorization(iaToken)) {
                ValidateAndPersist(RunValidations());
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
