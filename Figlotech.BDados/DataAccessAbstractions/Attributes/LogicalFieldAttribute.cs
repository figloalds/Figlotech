


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
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This attribute won't ever work until C# allows compiled functions in Attributes :\ 
    /// </summary>
    public sealed class LogicalFieldAttribute : Attribute
    {
        public Func<Object> LogicalFunction;
        public LogicalFieldAttribute(Func<Object> logicalFunction)
        {
            LogicalFunction = logicalFunction;
        }
    }
}
