using Figlotech.Core.BusinessModel;
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
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This is an example of ValidationAttribute
    /// </summary>
    public sealed class MinLengthAttribute : ValidationAttribute {
        readonly int MinLength;
        public MinLengthAttribute(int value) {
            MinLength = value;
        }

        public override ValidationErrors Validate(MemberInfo member, object value) {
            ValidationErrors retv = new ValidationErrors();
            if (value is String str) {
                if (str.Length > MinLength)
                    retv.Add(member.Name, $"{member.Name} should contain at least {MinLength}");
            }
            return retv;
        }

    }
}
