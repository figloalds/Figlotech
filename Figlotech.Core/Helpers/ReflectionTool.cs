using Figlotech.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public static class BadassReflectionExtensions {
        public static bool Implements(this Type t, Type interfaceType) {
            return ReflectionTool.TypeImplements(t, interfaceType);
        }
        public static bool DerivesFrom(this Type t, Type ancestorType) {
            return ReflectionTool.TypeDerivesFrom(t, ancestorType);
        }
        public static bool DerivesFromGeneric(this Type t, Type ancestorType) {
            return ReflectionTool.TypeDerivesFromGeneric(t, ancestorType);
        }
        public static IEnumerable<MemberInfo> AttributedMembersWhere<T>(this Type type, Func<MemberInfo, T, bool> act) where T : Attribute {
            var members = ReflectionTool.FieldsAndPropertiesOf(type);
            if (act == null) {
                return new List<MemberInfo>();
            }
            return members.Where(m => {
                var vatt = m.GetCustomAttribute<T>();
                return vatt != null && act.Invoke(m, vatt);
            });
        }
    }

    public class ReflectionTool {
        public static bool StrictMode { get; set; } = false;
        public static List<MemberInfo> FieldsAndPropertiesOf(Type type) {
            var members = new List<MemberInfo>();
            members.AddRange(type.GetFields());
            members.AddRange(type.GetProperties());
            return members;
        }

        public static bool TypeImplements(Type t, Type interfaceType) {
            return
                (t != null && interfaceType.IsInterface) &&
                t != typeof(Object) &&
                (t.GetInterfaces().Any(i => i == interfaceType) || TypeImplements(t.BaseType, interfaceType));
        }
        public static bool TypeDerivesFrom(Type t, Type ancestorType) {
            return
                t != null && t != typeof(Object) &&
                (t.BaseType == ancestorType || TypeDerivesFrom(t.BaseType, ancestorType));
        }
        public static bool TypeDerivesFromGeneric(Type t, Type ancestorType) {
            return 
                (t != null && t != typeof(Object)) &&
                (
                    t.IsGenericType && t.GetGenericTypeDefinition() == ancestorType || 
                    TypeDerivesFromGeneric(t.BaseType, ancestorType)
                );
        }

        public static IEnumerable<Type> GetLoadableTypesFrom(Assembly assembly) {
            // TODO: Argument validation
            try {
                return assembly.GetTypes();
            } catch (ReflectionTypeLoadException ex) {
                return ex.Types.Where(a => a != null);
            }
        }

        public static object RunGeneric(object input, MethodInfo method, Type type, params object[] args) {
            return method.MakeGenericMethod(type).Invoke(input, args);
        }

        public static object RunGeneric(object input, string methodName, Type type, params object[] args) {
            object retv = null;
            var methods = input.GetType().GetMethods().Where(
                (m) => m.Name == methodName);
            foreach (var a in methods) {
                try {
                    return a.MakeGenericMethod(type).Invoke(input, args);
                } catch (Exception x) {
                    //Fi.Tech.WriteLine(x.Message);
                }
            }
            return null;
        }

        public static object CreateGeneric(Type generic, Type arg, params object[] args) {
            return Activator.CreateInstance(generic.MakeGenericType(arg), args);
        }

        public static T GetAttributeFrom<T>(MemberInfo member) where T : Attribute {
            return member.GetCustomAttribute<T>();
        }

        public static MemberInfo GetMember(Type t, string fieldName, object target) {
            if (t == null) return null;
            MemberInfo member = ((MemberInfo)
                t
                    .GetFields()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault()) ?? ((MemberInfo)
                t
                    .GetProperties()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault());
            return member ?? GetMember(t.BaseType, fieldName, target);
        }

        public static bool SetValue(Object target, string fieldName, Object value) {
            try {
                MemberInfo member = GetMember(target?.GetType(), fieldName, target);
                if (member == null) {
                    return false;
                }
                SetMemberValue(member, target, value);
                return true;
            } catch (Exception x) {
                if (StrictMode) {
                    throw x;
                }
            }
            return false;
        }

        public static List<MemberInfo> FieldsWithAttribute<T>(Type type) where T : Attribute {
            var members = FieldsAndPropertiesOf(type);
            return members.Where((p) => p.GetCustomAttribute<T>() != null).ToList();
        }
        public static void ForAttributedMembers<T>(Type type, Action<MemberInfo, T> act) where T : Attribute {
            var members = FieldsAndPropertiesOf(type);
            members.ForEach(m => {
                    var att = m.GetCustomAttribute<T>();
                    if (att != null) {
                        act(m, att);
                    }
                });
        }
        public static IEnumerable<MemberInfo> AttributedMembersWhere<T>(Type type, Func<MemberInfo, T, bool> act) where T : Attribute {
            var members = FieldsAndPropertiesOf(type);
            if (act == null) {
                return new List<MemberInfo>();
            }
            return members.Where(m => {
                var vatt = m.GetCustomAttribute<T>();
                return vatt != null && act.Invoke(m, vatt);
            });
        }

        public static Object GetMemberValue(MemberInfo member, Object target) {
            if (member is PropertyInfo) {
                return ((PropertyInfo)member).GetValue(target);
            }
            if (member is FieldInfo) {
                return ((FieldInfo)member).GetValue(target);
            }
            return null;
        }

        static object ResolveEnum(Type t, object val) {
            if (!t.IsEnum)
                return val;
            int v;
            if (val is string s) {
                if (Int32.TryParse(s, out v)) {
                    return Enum.ToObject(t, v);
                } else {
                    return Enum.Parse(t, s);
                }
            }
            return val;
        }
        static Type ToUnderlying(Type t) {
            if (t == null) return null;
            return Nullable.GetUnderlyingType(t) ?? t;
        }

        public static object DbEvalValue(object value, Type t) {
            if (value is DBNull)
                return null;
            if (value == null)
                return null;
            t = ToUnderlying(t);
            value = ResolveEnum(t, value);

            return value;
        }

        public static object DbDeNull(object value) {
            if (value is DBNull)
                return null;
            if (value == null)
                return null;
            return value;
        }

        public static void SetMemberValue(MemberInfo member, Object target, Object value) {
            try {
                var pi = member as PropertyInfo;
                if (pi != null && pi.SetMethod == null) {
                    return;
                }

                var t = GetTypeOf(member);
                value = DbEvalValue(value, t);
                t = ToUnderlying(t);
                if (t == null) return;
                if (value == null && t.IsValueType) {
                    return;
                }

                if(t.IsEnum && value as int? != null) {
                    value = Enum.ToObject(t, (int) value);
                }

                if(value is string str && t == typeof(bool)) {
                    value = str.ToLower() == "true" || str.ToLower() == "yes" || str == "1";
                }

                if (value != null && !value.GetType().IsAssignableFrom(t)) {
                    value = Convert.ChangeType(value, t);
                }

                if (pi != null) {
                    pi.SetValue(target, value);
                    return;
                }
                if (member is FieldInfo fi) {
                    fi.SetValue(target, value);
                    return;
                }
            } catch (Exception x) {
                if (StrictMode) {
                    throw x;
                }
            }
        }

        public static Type GetTypeOf(MemberInfo member) {
            if (member is PropertyInfo) {
                return ((PropertyInfo)member).PropertyType;
            }
            if (member is FieldInfo) {
                return ((FieldInfo)member).FieldType;
            }
            return typeof(Object);
        }

        public static bool TypeContains(Type type, string field) {
            try {
                var members = new List<MemberInfo>();
                members.AddRange(type.GetFields());
                members.AddRange(type.GetProperties());
                foreach (var f in members) {
                    if (f.Name == field)
                        return true;
                }
                return false;
            } catch (Exception) {

            }
            return false;
        }

        public static Object GetValue(Object target, string fieldName) {
            try {
                var retv = GetMemberValue(GetMember(target?.GetType(), fieldName, target), target);
                return retv;
            } catch (Exception) {

            }
            return null;
        }

        public static void ForFields(Object target, Action<FieldInfo> function) {
            ForFields(target.GetType(), function);
        }
        public static void ForFields(Type type, Action<FieldInfo> function) {
            var fields = type.GetFields().ToArray();
            for (int x = 0; x < fields.Length; x++) {
                FieldInfo f = fields[x];
                try {
                    function(f);
                } catch (Exception) {
                    throw new ReflectionException($"Há um erro em uma função de reflexão ao acessar o campo {f.Name} de um objeto {f.DeclaringType.Name};");
                }
            }
        }

        public static void ForFieldsWithAttribute<T>(Object target, Action<FieldInfo, T> function) where T : Attribute, new() {
            ForFieldsWithAttribute<T>(target.GetType(), function);
        }
        public static void ForFieldsWithAttribute<T>(Type type, Action<FieldInfo, T> function) where T : Attribute, new() {
            var fields = type.GetFields().Where((a) => a.GetCustomAttribute<T>() != null).ToArray();
            for (int x = 0; x < fields.Length; x++) {
                FieldInfo f = fields[x];
                try {
                    function(f, f.GetCustomAttribute<T>());
                } catch (Exception) {
                    throw new ReflectionException($"Há um erro em uma função de reflexão ao acessar o campo {f.Name} de um objeto {f.DeclaringType.Name};");
                }
            }
        }
    }
}
