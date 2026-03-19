using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public static class RdbmsDataAccessorExtensions {
        public static async Task<bool> CheckStructure(this IRdbmsDataAccessor dataAccessor, Assembly ass) {
            return await CheckStructure(dataAccessor, ReflectionTool.GetLoadableTypesFrom(ass)).ConfigureAwait(false);
        }
        public static async Task<bool> CheckStructure(this IRdbmsDataAccessor dataAccessor, IEnumerable<Type> types) {
            var checker = new StructureChecker(dataAccessor, types);
            await checker.CheckStructureAsync().ConfigureAwait(false);
            return true;
        }

    }
}
