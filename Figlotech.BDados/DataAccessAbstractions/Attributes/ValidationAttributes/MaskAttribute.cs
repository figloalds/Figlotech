


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
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This is an example of ValidationAttribute
    /// </summary>
    public class MaskAttribute : ValidationAttribute
    {
        string Mask;
        public MaskAttribute(string value) {
            Mask = value;
        }

        public override ValidationErrors Validate(MemberInfo member, object value) {
            ValidationErrors retv = new ValidationErrors();
            if(value is String str) {
                if (!Regex.Match(str, Mask).Success)
                    retv.Add(member.Name, $"{member.Name} must match the defined mask {Mask}.");
            };
            return retv;
        }
    }
}
