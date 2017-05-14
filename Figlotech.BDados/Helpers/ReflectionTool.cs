using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers {
    public class ReflectionTool {

        public static List<MemberInfo> FieldsAndPropertiesOf(Type type) {
            var members = new List<MemberInfo>();
            members.AddRange(type.GetFields());
            members.AddRange(type.GetProperties());
            return members;
        }

        public static object RunGeneric(object input, MethodInfo method, Type type, params object[] args) {
            return method.MakeGenericMethod(type).Invoke(input, args);
        }

        public static object RunGeneric(object input, String methodName, Type type, params object[] args) {
            object retv = null;
            var methods = input.GetType().GetMethods().Where(
                (m) => m.Name == methodName);
            foreach(var a in methods) {
                try {
                    return a.MakeGenericMethod(type).Invoke(input, args);
                } catch (Exception x) {
                    Console.WriteLine(x.Message);
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
                target.GetType()
                    .GetFields()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault()?.SetValue(target, value);
                target.GetType()
                    .GetProperties()
                    .Where((field) => field.Name == fieldName)
                    .FirstOrDefault()?.SetValue(target, value);
                return true;
            } catch (Exception) {
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
        public static void SetMemberValue(MemberInfo member, Object target, Object value) {
            if (member is PropertyInfo) {
                ((PropertyInfo)member).SetValue(target, value);
            }                                            
            if (member is FieldInfo) {                   
                ((FieldInfo)member).SetValue(target, value);
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
                if(retv == null) {
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
