using Figlotech.BDados.Attributes;
using Figlotech.BDados.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public static class RdbmsDataAccessorExtensions
    {
        public static bool CheckStructure(this IRdbmsDataAccessor dataAccessor, Assembly ass) {
            return CheckStructure(dataAccessor, ass.GetTypes());
        }
        public static bool CheckStructure(this IRdbmsDataAccessor dataAccessor, IEnumerable<Type> types) {
            var checker = new StructureChecker(dataAccessor);
            checker.CheckStructure(types);
            return true;
        }

    }
}
