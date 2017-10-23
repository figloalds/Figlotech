


using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using Figlotech.Core.BusinessModel;
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

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This is an example of ValidationAttribute
    /// </summary>
    public class MinLengthAttribute : ValidationAttribute
    {
        int MinLength;
        public MinLengthAttribute(int value) {
            MinLength = value;
        }

        public override ValidationErrors Validate(MemberInfo member, object value) {
            ValidationErrors retv = new ValidationErrors();
            if(value is String str) {
                if (str.Length > MinLength)
                    retv.Add(member.Name, $"{member.Name} should contain at least {MinLength}");
            }
            return retv;
        }

    }
}
