


using Figlotech.BDados.Entity;
using Newtonsoft.Json;
/**
* Figlotech::Database::Entity::FieldAttribute
* Fields marked with this attribute will be automatically assigned
* at validation start according to their lambda function.
* 
*@Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Attributes {
    public class MaxLengthAttribute : ValidationAttribute
    {
        int MaxLength;
        public MaxLengthAttribute(int value) {
            MaxLength = value;
        }

        public override bool Validate(object value) {
            bool retv = true;
            Iae.As<string>(value, (str) => {
                if (str.Length > MaxLength)
                    retv = false;
            });
            return retv;
        }

        public override string GetValidationMessage(MemberInfo t) {
            return $"Maximum length for {t.Name} is {MaxLength}";
        }

    }
}
