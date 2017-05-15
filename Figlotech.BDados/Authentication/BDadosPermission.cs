using Figlotech.BDados.Attributes;
using Figlotech.BDados.Entity;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public class BDadosPermission : DataObject<BDadosPermission> {

        [Field(Size = 64, AllowNull = false)]
        public String User;

        [Field(Size = 64, AllowNull = false)]
        public String Form;

        [Field(AllowNull = false, DefaultValue = false)]
        public bool CanRead;

        [Field(AllowNull = false, DefaultValue = false)]
        public bool CanWrite;

        [Field(AllowNull = false, DefaultValue = false)]
        public bool CanDelete;

        [Field(AllowNull = false, DefaultValue = false)]
        public bool CanAuthorize;
    }
}
