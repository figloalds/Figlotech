using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
        public static MemberInfo[] GetFieldsAndProperties(this Type t) {
            return ReflectionTool.FieldsAndPropertiesOf(t);
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

        private static LenientDictionary<Type, MemberInfo[]> MembersCache { get; set; } = new LenientDictionary<Type, MemberInfo[]>();

        public static bool StrictMode { get; set; } = false;
        public static MemberInfo[] FieldsAndPropertiesOf(Type type) {
            if (type == null)
                throw new ArgumentNullException("Cannot get fields and properties of null type!");
            lock(String.Intern($"ACCESS_MEMBER_CACHE_{type.Name}")) {
                if (!MembersCache.ContainsKey(type)) {
                    MembersCache[type] = CollectMembers(type).ToArray();
                }
                // It is not clear rather the .Net Runtime caches or not 
                // the member info or if they do lookups all the time
                // It could be good to cache this, but I'm not sure yet.
                return MembersCache[type];
            }
        }

        private static IEnumerable<MemberInfo> CollectMembers(Type type) {
            foreach(var a in type.GetFields().Where(m => !m.IsStatic && m.IsPublic)) {
                yield return a;
            }
            foreach(var a in type.GetProperties().Where(
                m =>
                    (!m.GetGetMethod()?.IsStatic ?? true && (m.GetGetMethod()?.IsPublic ?? false)) ||
                    (!m.GetSetMethod()?.IsStatic ?? true && (m.GetSetMethod()?.IsPublic ?? false))
            )) {
                yield return a;
            }
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
            var methods = input.GetType().GetMethods().Where(
                (m) => m.Name == methodName);
            foreach (var a in methods) {
                try {
                    return a.MakeGenericMethod(type).Invoke(input, args);
                } catch (Exception x) {
                    if(Debugger.IsAttached) {
                        Debugger.Break();
                    }
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

        private static SelfInitializerDictionary<Type, Dictionary<string, MemberInfo>> MemberCacheFromString { get; set; } = new SelfInitializerDictionary<Type, Dictionary<string, MemberInfo>>(
            t => {
                if (t == null) return null;
                var retv = new Dictionary<string, MemberInfo>();
                var members = ReflectionTool.FieldsAndPropertiesOf(t);
                members.ForEach(x => {
                    retv[x.Name] = x;
                    retv[x.Name.ToLower()] = x;
                });
                return retv;
            }
        );
        //private static SelfInitializerDictionary<Type, MemberInfo[]> MemberCacheFromInt = new SelfInitializerDictionary<Type, MemberInfo[]>(
        //    t => {
        //        if (t == null) return null;
        //        var members = ReflectionTool.FieldsAndPropertiesOf(t);
        //        var retv = new MemberInfo[members.Length];
        //        for(int i = 0; i < members.Length; i++) {
        //            retv[i] = members[i];
        //        }

        //        return retv;
        //    }
        //);

        public static MemberInfo GetMember(Type t, string fieldName) {
            return MemberCacheFromString[t][fieldName];
        }
        public static MemberInfo GetMember(Type t, int i) {
            return MembersCache[t][i];
        }

        public static bool SetValue(Object target, string fieldName, Object value) {
            try { 
                MemberInfo member = GetMember(target?.GetType(), fieldName);
                if (member == null) {
                    return false;
                }
                SetMemberValue(member, target, value);
                return true;
            } catch (Exception x) {
                //if(Debugger.IsAttached) {
                //    Debugger.Break();
                //}
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
            if (member is PropertyInfo pi) {
                return pi.GetValue(target);
            }
            if (member is FieldInfo fi) {
                return fi.GetValue(target);
            }
            return null;
        }

        static object ResolveEnum(Type t, object val) {
            if (!t.IsEnum)
                return val;
            if (val is string s) {
                if (Int32.TryParse(s, out int v)) {
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

        public static object TryCast(object value, Type t) {
            try {
                value = DbEvalValue(value, t);
                t = ToUnderlying(t);
                if (t == null)
                    return null;

                if (value == null) {
                    if (t.IsValueType) {
                        return Activator.CreateInstance(t);
                    }
                    return null;
                }

                if (t.IsGenericType) {
                    if (t.GetGenericTypeDefinition() == typeof(FnVal<>)) {
                        t = t.GetGenericArguments()[0];
                    }
                }

                if (t.IsEnum && value as int? != null) {
                    return Enum.ToObject(t, (int)value);
                }

                if (value is string str && t == typeof(bool)) {
                    return str.ToLower() == "true" || str.ToLower() == "yes" || str == "1";
                }

                if (!t.IsAssignableFrom(value.GetType())) {
                    if (value.GetType().Implements(typeof(IConvertible))) {
                        return Convert.ChangeType(value, t);
                    }
                }

                if (value.GetType() == typeof(byte[])) {
                    var vbarr = value as byte[];

                    if (t == typeof(Boolean))
                        return vbarr[0] != 0 && vbarr[0] != '0';
                    if (t == typeof(Int16))
                        return BitConverter.ToInt16(Fi.Tech.BinaryPad(vbarr, sizeof(Int16)), 0);
                    if (t == typeof(Int32))
                        return BitConverter.ToInt32(Fi.Tech.BinaryPad(vbarr, sizeof(Int32)), 0);
                    if (t == typeof(Int64))
                        return BitConverter.ToInt64(Fi.Tech.BinaryPad(vbarr, sizeof(Int64)), 0);
                    if (t == typeof(UInt16))
                        return BitConverter.ToUInt16(Fi.Tech.BinaryPad(vbarr, sizeof(UInt16)), 0);
                    if (t == typeof(UInt32))
                        return BitConverter.ToUInt32(Fi.Tech.BinaryPad(vbarr, sizeof(UInt32)), 0);
                    if (t == typeof(UInt64))
                        return BitConverter.ToUInt64(Fi.Tech.BinaryPad(vbarr, sizeof(UInt64)), 0);
                    if (t == typeof(Double))
                        return BitConverter.ToDouble(Fi.Tech.BinaryPad(vbarr, sizeof(Double)), 0);
                    if (t == typeof(Decimal))
                        return (decimal)BitConverter.ToDouble(Fi.Tech.BinaryPad(vbarr, sizeof(Double)), 0);
                    if (t == typeof(Single))
                        return BitConverter.ToSingle(Fi.Tech.BinaryPad(vbarr, sizeof(Single)), 0);

                    if (t == typeof(DateTime)) {
                        var vbarrStr = new UTF8Encoding(false).GetString(vbarr);
                        if(vbarr.Length <= sizeof(Int64)) {
                            var v64 = BitConverter.ToInt64(Fi.Tech.BinaryPad(vbarr, sizeof(Int64)), 0);
                            DateTime dt2;
                            if(v64 > DateTime.MinValue.Ticks && v64 < DateTime.MaxValue.Ticks) {
                                dt2 = new DateTime(v64);
                            } else  {
                                dt2 = new DateTime(v64 * TimeSpan.TicksPerMillisecond);
                            }
                            return dt2;
                        }
                        var dt = DateTime.Parse(vbarrStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUniversalTime();
                        return dt;
                    }

                    if (t == typeof(string)) {
                        return new UTF8Encoding(false).GetString(value as byte[]);
                    }
                    if (t.IsEnum) {
                        var vEnnyVals = Enum.GetValues(t);
                        foreach (var v in vEnnyVals) {
                            var bi32 = BitConverter.ToInt32(Fi.Tech.BinaryPad(vbarr, sizeof(Int32)), 0);
                            if (bi32 == (int)v) {
                                return v;
                            }
                        }
                        var strEnum = new UTF8Encoding(false).GetString(value as byte[]);
                        try {
                            return Enum.Parse(t, strEnum);
                        } catch (Exception) {

                            if (Debugger.IsAttached) {
                                Debugger.Break();
                            }
                        }
                    }
                }

                return Activator.CreateInstance(t);
            } catch (Exception x) {

                throw x;
            }
        }

        public static void SetMemberValue(MemberInfo member, Object target, Object value) {
            try {
                var pi = member as PropertyInfo;
                var fi = member as FieldInfo;
                if (pi != null && pi.SetMethod == null) {
                    return;
                }
                if (fi != null && fi.IsInitOnly) {
                    return;
                }

                var t = GetTypeOf(member);
                if(value != null && t == value.GetType()) {
                    pi?.SetValue(target, value);
                    fi?.SetValue(target, value);
                    return;
                }
                value = DbEvalValue(value, t);
                t = ToUnderlying(t);
                if (t == null) return;

                if (value == null) {
                    if (t.IsValueType) {
                        return;
                    } else {
                        if (pi != null) {
                            pi.SetValue(target, null);
                            return;
                        }

                        if (fi != null) {
                            fi.SetValue(target, null);
                            return;
                        }
                    }
                    return;
                }


                if (t.IsGenericType) {
                    if (t.GetGenericTypeDefinition() == typeof(FnVal<>)) {
                        target = GetMemberValue(member, target);
                        member = t.GetProperty("Value");
                        t = t.GetGenericArguments()[0];
                    }
                }

                if (t.IsEnum && value as int? != null) {
                    value = Enum.ToObject(t, (int)value);
                }

                if (value is string str && t == typeof(bool)) {
                    value = str.ToLower() == "true" || str.ToLower() == "yes" || str == "1";
                }

                if (!t.IsAssignableFrom(value.GetType())) {
                    if (value.GetType().Implements(typeof(IConvertible))) {
                        value = Convert.ChangeType(value, t);
                    } else {
                        return;
                    }
                }

                if (pi != null) {
                    pi.SetValue(target, value);
                    return;
                }

                if (fi != null) {
                    fi.SetValue(target, value);
                }

            } catch (Exception x) {
                //if (Debugger.IsAttached) {
                //    Debugger.Break();
                //}
                if (StrictMode) {
                    Console.WriteLine($"RTSV Error | {target?.GetType()?.Name}::{member?.Name}={value?.ToString()} ({value?.GetType()?.Name})");
                    if (Debugger.IsAttached) {
                        Debugger.Break();
                    }
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

                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
            }
            return false;
        }

        public static Object GetValue(Object target, string fieldName) {
            try {
                var retv = GetMemberValue(GetMember(target?.GetType(), fieldName), target);
                return retv;
            } catch (Exception) {

                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
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

        public static bool DoesTypeHaveFieldOrProperty(Type type, string key) {
            return MemberCacheFromString[type].ContainsKey(key);
        }
    }
}
