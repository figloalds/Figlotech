using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Tests.StructureCheckMocks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Tests
{

    public class client : CustomBaseDataObject {
        [PrimaryKey]
        [Field(PrimaryKey = true)]
        public long cli_id;
        [Field(Size = 100)]
        public string cli_name;
        [Field(AllowNull = true, Size = 50)]
        public string cli_gender;
        [Field(AllowNull = true)]
        public DateTime? cli_birthdate;
        [Field(AllowNull = true, Size = 100)]
        public string cli_address;
        [Field(AllowNull = true, Size = 50)]
        public string cli_city;
        [Field(AllowNull = true, Size = 50)]
        public string cli_postalcode;
        [Field(AllowNull = true, Size = 100)]
        public string cli_email;
        [Field(AllowNull = true, Size = 50)]
        public string cli_phone;
    }
}
