using Figlotech.BDados.Attributes;
using Figlotech.BDados.Entity;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public class BDadosUserSession : DataObject {
        [Field(Size = 64, AllowNull = false)]
        public String User;

        [Field(Size = 256, AllowNull = false)]
        public String Token;

        [Field(AllowNull = false, DefaultValue = true)]
        public bool isActive = true;

        [Field(AllowNull = false)]
        public DateTime StartTime = DateTime.UtcNow;

        [Field(AllowNull = true)]
        public DateTime? EndTime;

    }
}
