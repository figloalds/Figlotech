


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
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.Attributes {
    public class MaskAttribute : ValidationAttribute
    {
        string Mask;
        public MaskAttribute(string value) {
            Mask = value;
        }

        public override bool Validate(object value) {
            bool retv = true;
            FTH.As<string>(value, (str) => {
                if(!Regex.Match(str, Mask).Success)
                    retv = false;
            });
            return retv;
        }

        public override string GetValidationMessage(MemberInfo t) {
            return $"{t.Name} must match the given mask.";
        }

    }
}
