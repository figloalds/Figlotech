using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Figlotech.Core
{
    public static class ValuePathReader
    {
        static SelfInitializerDictionary<Type, LenientDictionary<string, PropertyInfo>> PropertyCache =
            new SelfInitializerDictionary<Type, LenientDictionary<string, PropertyInfo>>(
                t => {
                    return (LenientDictionary<string, PropertyInfo>)t
                        .GetProperties()
                        .ToDictionary(p => p.Name, p => p);
                }
            );
        static SelfInitializerDictionary<Type, MethodInfo> SquareMethodsStringCache =
            new SelfInitializerDictionary<Type, MethodInfo>(
                t => {
                    return t
                        .GetMethods()
                        .FirstOrDefault(
                            x => x.Name == "get_Item" && 
                                x.GetParameters().Length == 1 && 
                                x.GetParameters()[0].ParameterType == typeof(String)
                            );
                }
            ) {
                AllowNullValueCaching = true
            };
        static SelfInitializerDictionary<Type, MethodInfo> SquareMethodsIntCache =
            new SelfInitializerDictionary<Type, MethodInfo>(
                t => {
                    return t
                        .GetMethods()
                        .FirstOrDefault(
                            x => x.Name == "get_Item" && 
                                x.GetParameters().Length == 1 && 
                                x.GetParameters()[0].ParameterType == typeof(Int32)
                            );
                }
            ) {
                AllowNullValueCaching = true
            };


        public static object Read(object input, string path) {
            var retv = input;
            if (input != null) {
                if (path.StartsWith("/"))
                    path = path.Substring(1);
                if (path.EndsWith("/"))
                    path = path.Substring(0, path.Length - 1);
                do {
                    var idx = path.IndexOf('/');
                    string current;
                    if (idx >= 0) {
                        current = (path.Substring(0, idx));
                        path = (path.Substring(idx + 1, path.Length - idx - 1));
                    } else {
                        current = path;
                        path = null;
                    }
                    var type = retv.GetType();
                    try {
                        var squareGetterString = SquareMethodsStringCache[type];
                        var squareGetterInt = SquareMethodsIntCache[type];
                        if (squareGetterString != null) {
                            retv = squareGetterString.Invoke(retv, new object[] { current });
                        } else
                        if (squareGetterInt != null && Int32.TryParse(current, out var currentI)) {
                            retv = squareGetterInt.Invoke(retv, new object[] { currentI });
                        } else {
                            var prop = PropertyCache[type][current];
                            if (prop != null) {
                                retv = prop.GetValue(retv);
                            } else {
                                return null;
                            }
                        }
                    }
                    // The ValuePathReader is meant to be lenient
                    // If the get-value logic throws a known exception,
                    // we ignore it and return null
                    // This is done on purpose and by design
                    catch(TargetInvocationException) {
                        return null;
                    } catch (IndexOutOfRangeException) {
                        return null;
                    } catch(ArgumentOutOfRangeException) {
                        return null;
                    }
                } while (!string.IsNullOrEmpty(path));
            }
            return retv;
        }
    }
}
