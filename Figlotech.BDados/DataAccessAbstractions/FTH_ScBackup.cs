using System;
using System.Collections.Generic;
using System.Text;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core.BusinessModel;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class _ResidualScData : DataObject<_ResidualScData> {
        [Field(Size = 128)]
        public string Type { get; set; }
        [Field(Size = 128)]
        public string Field { get; set; }
        [Field(Type="BLOB", AllowNull = true)]
        public byte[] Value { get; set; }
        [Field(Size = 64)]
        public string ReferenceRID { get; set; }
        [Field(Size = 64)]
        public override ulong AlteredBy { get; set; }
        [Field(Size = 64)]
        public override ulong CreatedBy { get; set; }

        protected override IEnumerable<IValidationRule<_ResidualScData>> ValidationRules() {
            yield break;
        }
    }
}
