using Figlotech.BDados.Builders;
using Figlotech.Core;
using Figlotech.Core.Interfaces;
using Newtonsoft.Json;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {
    public abstract class BaseDataObject<TId> : IDataObject<TId> {

        protected BaseDataObject(IDataAccessor dataAccessor, IContextProvider ctxProvider) {
            DataAccessor = dataAccessor;
            ContextProvider = ctxProvider;
        }

        protected BaseDataObject() {
        }

        public override string ToString() {
            return Convert.ToString(Id);
        }

        public abstract TId Id { get; set; }
        public virtual DateTime CreatedAt { get; set; }
        public virtual DateTime? UpdatedAt { get; set; }

        object IDataObject.Id {
            get {
                return Id;
            }
            set {
                if (value == null) {
                    Id = default(TId);
                    return;
                }
                if (value is TId typedValue) {
                    Id = typedValue;
                    return;
                }
                var targetType = typeof(TId);
                if (targetType == typeof(Guid)) {
                    if (value is string guidText) {
                        Id = (TId)(object)new Guid(guidText);
                        return;
                    }
                    if (value is Guid guidValue) {
                        Id = (TId)(object)guidValue;
                        return;
                    }
                }
                if (targetType.IsEnum) {
                    Id = (TId)Enum.Parse(targetType, Convert.ToString(value));
                    return;
                }
                Id = (TId)Convert.ChangeType(value, targetType);
            }
        }

        [JsonIgnore]
        public virtual IDataAccessor DataAccessor { get; set; }

        [JsonIgnore]
        public IContextProvider ContextProvider { get; set; }
    }
}
