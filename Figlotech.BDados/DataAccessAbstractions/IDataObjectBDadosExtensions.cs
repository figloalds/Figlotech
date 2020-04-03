using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public static class IDataObjectBDadosExtensions {
        public static T Duplicate<T>(this T origin) where T : IDataObject, new() {
            if (origin == null)
                return default(T);
            var t = typeof(T);
            var ridCol = FiTechBDadosExtensions.RidColumnOf[t];
            var idCol = FiTechBDadosExtensions.IdColumnOf[t];
            T destination = new T();
            ObjectReflector.Open(origin, (objA) => {
                ObjectReflector.Open(destination, (objB) => {
                    foreach (var field in objB) {
                        if (field.Key.Name == ridCol) {
                            continue;
                        }
                        if (objA.ContainsKey(field.Key.Name)) {
                            objB[field.Key] = objA[field.Key.Name];
                        }
                    }
                });
            });

            return destination;
        }
    }
}
