using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public class ReflectionTool {
        public static bool StrictMode { get; set; } = false;
        public static List<MemberInfo> FieldsAndPropertiesOf(Type type) {
            var members = new List<MemberInfo>();
            members.AddRange(type.GetFields());
            members.AddRange(type.GetProperties());
            return members;
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

        public static object RunGeneric(object input, String methodName, Type type, params object[] args) {
            object retv = null;
            var methods = input.GetType().GetMethods().Where(
                (m) => m.Name == methodName);
            foreach (var a in methods) {
                try {
                    return a.MakeGenericMethod(type).Invoke(input, args);
                } catch (Exception x) {
                    Fi.Tech.WriteLine(x.Message);
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

        public static bool SetValue(Object target, String fieldName, Object value) {
            try {
                MemberInfo member = ((MemberInfo)
                target.GetType()
                    .GetFields()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault()) ?? ((MemberInfo)
                target.GetType()
                    .GetProperties()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault());
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

                var t = GetTypeOf(member);
                value = DbEvalValue(value, t);
                t = ToUnderlying(t);
                if (value == null && t.IsValueType) {
                    return;
                }
                if (t == typeof(bool) || t == typeof(Boolean)) {
                    if (value is String str) {
                        if (str.ToUpper() == "YES")
                            value = true;
                        if (str.ToUpper() == "NO")
                            value = false;
                    }
                }
                if (value != null && !value.GetType().IsAssignableFrom(t)) {
                    value = Convert.ChangeType(value, t);
                }
                if (member is PropertyInfo pi) {
                    ((PropertyInfo)member).SetValue(target, value);
                }
                if (member is FieldInfo fi) {
                    ((FieldInfo)member).SetValue(target, value);
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

        public static bool TypeContains(Type type, String field) {
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

        public static Object GetValue(Object target, String fieldName) {
            try {
                var retv = target.GetType()
                    .GetFields()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault()?.GetValue(target);
                if (retv == null) {
                    retv = target.GetType()
                    .GetProperties()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault()?.GetValue(target);
                }
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
