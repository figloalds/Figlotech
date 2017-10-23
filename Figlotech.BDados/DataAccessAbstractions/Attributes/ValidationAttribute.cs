/**
* Figlotech::Database::Entity::FieldAttribute
* Fields marked with this attribute will be automatically assigned
* at validation start according to their lambda function.
* 
*@Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using Figlotech.Core.BusinessModel;
using System;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Basic abstract ValidationAttribute, a work in progress.
    /// </summary>
    public abstract class ValidationAttribute : Attribute
    {
        public abstract ValidationErrors Validate(MemberInfo member, object value);
    }
}
