using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.BusinessModel;
using System.Threading.Tasks;
using Figlotech.Core.Interfaces;

namespace Figlotech.BDados.DataAccessAbstractions {
    public abstract class DataObject<T> : BaseDataObject, IBusinessObject where T: ILegacyDataObject, IBusinessObject, new() {

        [Field()]
        [PrimaryKey]
        public override long Id { get; set; } = 0;

        [Field(AllowNull = true, DefaultValue = null)]
        public override DateTime? UpdatedTime { get; set; } = Fi.Tech.GetUtcTime();

        [NoUpdate]
        [Field(Type = "TIMESTAMP", AllowNull = false, DefaultValue = "CURRENT_TIMESTAMP")]
        public override DateTime CreatedTime { get; set; } = Fi.Tech.GetUtcTime();

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

        public override async Task<ValidationErrors> Validate() {
            await Task.Yield();
            ValidationErrors ve = new ValidationErrors();
            var myType = this.GetType();

            T obj = obj = (T)(IBusinessObject)this;

            foreach (var a in ValidationRules()) {
                foreach (var err in a.Validate(obj)) {
                    ve.Add(err);
                }
            }

            // Validations
            var validations = ReflectionTool.GetAttributedMemberValues<ValidationAttribute>(myType);
            for(int i = 0; i < validations.Length; i++) {
                var field = validations[i].Member;
                var vAttribute = validations[i].Attribute;
                if (vAttribute != null) {
                    foreach (var a in vAttribute.Validate(field, ReflectionTool.GetMemberValue(field, this)))
                        ve.Add(a);
                }
            }

            return ve;
        }

        async Task CascadingDoForFields<T>(Func<T, Task> process, List<Type> prevList = null) {

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
                    await CascadingDoForFields<T>(process, prevList);
                }
                if (!ReflectionTool.GetTypeOf(field).GetInterfaces().Contains(typeof(T)))
                    continue;
                prevList.Add(ReflectionTool.GetTypeOf(field));
                try {
                    T workObject = ((T)workingValue);
                    await process(workObject);
                }
                catch (Exception) { }
            }
        }

        InstanceAuthorizer ia = new InstanceAuthorizer();
        public async Task<string> RunValidations() {

            var errors = new ValidationErrors();

            errors.Merge(await this.Validate());
            errors.Merge(await this.ValidateInput());
            errors.Merge(await this.ValidateBusiness());

            await CascadingDoForFields<IBusinessObject>(async (field) => {
                errors.Merge(await field?.ValidateInput());
                errors.Merge(await field?.ValidateBusiness());
            });

            if (errors.Count > 0) {
                throw new BusinessValidationException(errors);
            }

            return ia.GenerateAuthorization();
        }

        public virtual async Task<bool> ValidateAndPersistAsync(String iaToken) {
            if (!ia.CheckAuthorization(iaToken)) {
                await ValidateAndPersistAsync(await RunValidations()).ConfigureAwait(false);
            }

            if (null == DataAccessor) {
                throw new BDadosException($"Data Accessor is undefined in this instance of {this.GetType().Name}");
            }
            await this.OnBeforePersistAsync().ConfigureAwait(false);

            var retv = true;

            retv &= DataAccessor.SaveItem(this);

            await this.OnAfterPersistAsync().ConfigureAwait(false);

            return retv;
        }
    }
}
