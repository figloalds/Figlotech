


using Figlotech.BDados.DataAccessAbstractions;
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
    /// <summary>
    /// Basic abstract ValidationAttribute, a work in progress.
    /// </summary>
    public abstract class ValidationAttribute : Attribute
    {
        public abstract ValidationErrors Validate(MemberInfo member, object value);
    }
}
