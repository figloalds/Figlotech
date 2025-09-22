using Figlotech.Core.Extensions;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
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
        public static Type AsDerivingFromGeneric(this Type t, Type ancestorType) {
            return ReflectionTool.TypeAsDerivingFromGeneric(t, ancestorType);
        }
        public static MemberInfo[] GetFieldsAndProperties(this Type t) {
            return ReflectionTool.FieldsAndPropertiesOf(t);
        }
        public static IEnumerable<Type> BaseTypeTree(this Type t) {
            yield return t;
            if (t != typeof(Object) && t.BaseType != null) {
                foreach (var a in BaseTypeTree(t.BaseType)) {
                    yield return a;
                }
            }
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

    public sealed class ReflectionTool {
        
        private static LenientDictionary<Type, MemberInfo[]> MembersCache { get; set; } = new LenientDictionary<Type, MemberInfo[]>();

        public static bool StrictMode { get; set; } = false;
        public static MemberInfo[] FieldsAndPropertiesOf(Type type) {
            if (type == null)
                throw new ArgumentNullException("Cannot get fields and properties of null type!");
            lock (String.Intern($"ACCESS_MEMBER_CACHE_{type.Name}")) {
                if (!MembersCache.ContainsKey(type)) {
                    MembersCache[type] = CollectMembers(type).ToArray();
                }
                // It is not clear rather the .Net Runtime caches or not 
                // the member info or if they do lookups all the time
                // It could be good to cache this, but I'm not sure yet.
                return MembersCache[type];
            }
        }

        static ConcurrentDictionary<(Type, Type), object> AttributedMembersCache = new ConcurrentDictionary<(Type, Type), object>();
        public static (MemberInfo Member, TAttribute Attribute)[] GetAttributedMemberValues<TAttribute>(Type t) where TAttribute : Attribute {
            return AttributedMembersCache.GetOrAdd((t, typeof(TAttribute)), 
                _ => InitAttributedMembersCache<TAttribute>(t)) as (MemberInfo, TAttribute)[];
        }

        private static (MemberInfo, TAttribute)[] InitAttributedMembersCache<TAttribute>(Type t) where TAttribute : Attribute {
            var fields = t.GetFields();
            var properties = t.GetProperties();
            var retv = new List<(MemberInfo, TAttribute)>();
            for (int i = 0; i < fields.Length; i++) {
                var att = fields[i].GetCustomAttribute<TAttribute>();
                if (att != null) {
                    retv.Add((fields[i], att));
                }
            }
            for (int i = 0; i < properties.Length; i++) {
                var att = properties[i].GetCustomAttribute<TAttribute>();
                if (att != null) {
                    retv.Add((properties[i], att));
                }
            }

            return retv.ToArray();
        }

        private static IEnumerable<MemberInfo> CollectMembers(Type type) {
            foreach (var a in type.GetFields(BindingFlags.Instance).Where(m => !m.IsStatic && m.IsPublic)) {
                yield return a;
            }
            foreach (var a in type.GetFields().Where(m => !m.IsStatic && m.IsPublic)) {
                yield return a;
            }
            foreach (var a in type.GetProperties(BindingFlags.Instance).Where(
                m =>
                    (!(m.GetGetMethod()?.IsStatic ?? true) && (m.GetGetMethod()?.IsPublic ?? false)) ||
                    (!(m.GetSetMethod()?.IsStatic ?? true) && (m.GetSetMethod()?.IsPublic ?? false))
            )) {
                yield return a;
            }
            foreach (var a in type.GetProperties().Where(
                m =>
                    (!(m.GetGetMethod()?.IsStatic ?? true) && (m.GetGetMethod()?.IsPublic ?? false)) ||
                    (!(m.GetSetMethod()?.IsStatic ?? true) && (m.GetSetMethod()?.IsPublic ?? false))
            )) {
                yield return a;
            }
        }

        public static bool TypeImplements(Type t, Type interfaceType) {
            return
                (t != null && interfaceType.IsInterface) &&
                t != typeof(Object) &&
                (
                    t.GetInterfaces().Any(i => i == interfaceType || (i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType)) ||
                    TypeImplements(t.BaseType, interfaceType)
                );
        }

        public static bool TypeDerivesFrom(Type t, Type ancestorType) {
            return
                t != null && t != typeof(Object) &&
                (t.BaseType == ancestorType || TypeDerivesFrom(t.BaseType, ancestorType));
        }
        public static Type? TypeAsDerivingFromGeneric(Type t, Type ancestorType) {
            if(t == typeof(Object)) {
                return null;
            }
            return (t != null && t != typeof(Object)) &&
                (
                    t.IsGenericType && t.GetGenericTypeDefinition() == ancestorType
                    
                ) ? t : TypeAsDerivingFromGeneric(t.BaseType, ancestorType);
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
                    if (Debugger.IsAttached) {
                        Debugger.Break();
                    }
                }
            }
            return null;
        }

        public static object CreateGeneric(Type generic, Type arg, params object[] args) {
            return Activator.CreateInstance(generic.MakeGenericType(arg), args);
        }

        public static bool IsListOfT(Type type) {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>);
        }
        public static Type GetListElementType(object list) {
            return list.GetType().GetGenericArguments()[0];
        }

        private static SelfInitializerDictionary<Type, MethodInfo> ListEnumeratorCache = new SelfInitializerDictionary<Type, MethodInfo>(
            t => t.GetMethod("GetEnumerator")
        );
        private static SelfInitializerDictionary<Type, MethodInfo> EnumeratorMoveNextCache = new SelfInitializerDictionary<Type, MethodInfo>(
            t => t.GetMethod("MoveNext")
        );
        private static SelfInitializerDictionary<Type, PropertyInfo> EnumeratorPropCurrentCache = new SelfInitializerDictionary<Type, PropertyInfo>(
            t => t.GetProperty("Current")
        );

        public static IEnumerable<object> EnumerateList(object list) {
            if(list == null) {
                yield break;
            }
            var enumerator = ListEnumeratorCache[list.GetType()].Invoke(list, Array.Empty<object>());
            var enumeratorType = enumerator.GetType();
            var moveNext = EnumeratorMoveNextCache[enumeratorType];
            while ((bool) moveNext.Invoke(enumerator, Array.Empty<object>())) {
                yield return EnumeratorPropCurrentCache[enumeratorType];
            }
        }

        static ConcurrentDictionary<(MemberInfo, Type), Attribute[]> MemberAttributesByTypeCache = new ConcurrentDictionary<(MemberInfo, Type), Attribute[]>();
        public static T GetAttributeFrom<T>(MemberInfo member) where T : Attribute {
            return MemberAttributesByTypeCache.GetOrAddWithLocking((member, typeof(T)), (k) => {
                return member.GetCustomAttributes<T>().ToArray();
            })?.FirstOrDefault() as T;
        }

        private static SelfInitializerDictionary<Type, LenientDictionary<string, MemberInfo>> MemberCacheFromString { get; set; } = new SelfInitializerDictionary<Type, LenientDictionary<string, MemberInfo>>(
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

        public static bool SetValueAndConfirmChange(Object target, string fieldName, Object value) {
            var oldValue = GetValue(target, fieldName);
            ReflectionTool.SetValue(target, fieldName, value);
            var newValue = GetValue(target, fieldName);
            if ((newValue == null && oldValue == null)) {
                return false;
            }
            return (
                (newValue == null ^ oldValue == null) ||
                (newValue is IComparable ic && newValue.Equals(oldValue)) ||
                (newValue!.Equals(oldValue))
            );
        }

        public static bool SetValue(Object target, string fieldName, Object value) {
            MemberInfo member = GetMember(target?.GetType(), fieldName);
            if (member == null) {
                //Debugger.Break();
                return false;
            }
            SetMemberValue(member, target, value);
            return true;
        }

        public static List<MemberInfo> FieldsWithAttribute<T>(Type type) where T : Attribute {
            return GetAttributedMemberValues<T>(type).Select(x => x.Member).ToList();
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
            if (member == null) {
                return null;
            }
            if (member is PropertyInfo pi && pi.GetMethod != null) {
                if (pi.GetIndexParameters().Length == 0) {
                    try {
                        return pi.GetValue(target);
                    } catch (Exception x) {
                        return null;
                    }
                } else {
                    return null;
                }
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
        static (bool IsNullable, Type UnderlyingType) ToUnderlying(Type t) {
            if (t == null) return (true, null);
            var underlyingFromNullable = Nullable.GetUnderlyingType(t);

            return (underlyingFromNullable != null, underlyingFromNullable ?? t);
        }

        public static object DbEvalValue(object value, Type t) {
            if (value is DBNull)
                return null;
            if (value == null)
                return null;
            (_, t) = ToUnderlying(t);
            value = ResolveEnum(t, value);

            return value;
        }

        public static object DbDeNull(object value) {
            if (value is DBNull || value == null) {
                return null;
            }

            return value;
        }

        public static object TryCast(object value, Type t) {
            try {
                value = DbEvalValue(value, t);
                (var typeIsNullable, t) = ToUnderlying(t);
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
                } else {
                    if (value is string str && t == typeof(bool)) {
                        return str.ToLower() == "true" || str.ToLower() == "yes" || str == "1";
                    }

                    if (!t.IsAssignableFrom(value.GetType())) {
                        if (value.GetType().Implements(typeof(IConvertible))) {
                            return Convert.ChangeType(value, t);
                        }
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
                        if (vbarr.Length <= sizeof(Int64)) {
                            var v64 = BitConverter.ToInt64(Fi.Tech.BinaryPad(vbarr, sizeof(Int64)), 0);
                            DateTime dt2;
                            if (v64 > DateTime.MinValue.Ticks && v64 < DateTime.MaxValue.Ticks) {
                                dt2 = new DateTime(v64);
                            } else {
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


        static SelfInitializerDictionary<Type, LenientDictionary<Type, MethodInfo>> CastMethods = new SelfInitializerDictionary<Type, LenientDictionary<Type, MethodInfo>>(
            fieldType => {
                var retv = new LenientDictionary<Type, MethodInfo>();

                var src = fieldType.GetMethods(BindingFlags.Public | BindingFlags.Static)
                        .Where(mi => (mi.Name == "op_Implicit" || mi.Name == "op_Explicit") && mi.ReturnType == fieldType);

                foreach (var mi in src) {
                    ParameterInfo pi = mi.GetParameters().FirstOrDefault();
                    if (pi?.ParameterType != null) {
                        retv[pi.ParameterType] = (MethodInfo)mi;
                    }
                }

                return retv;
            }
        );

        private static void _setMemberValueInternal(MemberInfo member, object target, Object value) {
            if (_setterMethodCache.TryGetValue(member, out var method)) {
                if (method != null) {
                    method(target, value);
                }
            }

            var pi = member as PropertyInfo;
            var fi = member as FieldInfo;
            if (pi != null && pi.SetMethod == null) {
                return;
            }
            if (fi != null && fi.IsInitOnly) {
                return;
            }

            if (pi != null) {
                _setterMethodCache[member] = pi.SetValue;
                pi.SetValue(target, value);
            }

            if (fi != null) {
                _setterMethodCache[member] = fi.SetValue;
                fi.SetValue(target, value);
            }
        }

        // ====== CACHES ======
        private static readonly ConcurrentDictionary<(Type Src, Type Dest), Delegate> _converterCache
            = new();

        // ====== PUBLIC API ======
        private static readonly Type[] _dbSourceCandidates = new[] {
            typeof(string),
            typeof(int), typeof(long), typeof(short), typeof(byte), typeof(sbyte),
            typeof(uint), typeof(ulong), typeof(ushort),
            typeof(decimal), typeof(double), typeof(float),
            typeof(DateTime), typeof(DateTimeOffset), typeof(TimeSpan),
            typeof(Guid),
            typeof(byte[]) // least likely for user operators, but harmless to keep
        };
        private static bool IsProviderNative(Type t) {
            return t == typeof(bool)
                || t == typeof(byte) || t == typeof(sbyte)
                || t == typeof(short) || t == typeof(ushort)
                || t == typeof(int) || t == typeof(uint)
                || t == typeof(long) || t == typeof(ulong)
                || t == typeof(float) || t == typeof(double)
                || t == typeof(decimal)
                || t == typeof(string)
                || t == typeof(DateTime)
                || t == typeof(DateTimeOffset)
                || t == typeof(Guid)
                || t == typeof(byte[])
                || t == typeof(TimeSpan);
        }
        private static readonly ConcurrentDictionary<Type, Type> _bestSourceTypeCache = new();
        public static Type GetBestSourceTypeFor(Type targetType) {
            if (targetType == null) throw new ArgumentNullException(nameof(targetType));

            // Unwrap Nullable<T>
            var underlying = Nullable.GetUnderlyingType(targetType);
            if (underlying != null) targetType = underlying;

            // Enums: read as underlying integral type
            if (targetType.IsEnum)
                return Enum.GetUnderlyingType(targetType);

            // Directly provider-native? Just use it.
            if (IsProviderNative(targetType))
                return targetType;

            // User-defined conversion from a provider type?
            return _bestSourceTypeCache.GetOrAdd(targetType, t =>
            {
                // Scan candidates; if an implicit/explicit operator exists from candidate -> t, pick it.
                foreach (var cand in _dbSourceCandidates) {
                    if (TryGetUserDefinedConversion(cand, t, out _))
                        return cand;
                }

                // Fallback: try exact target (some providers support UDT GetFieldValue<T>)
                // Otherwise, use string (safest + most broadly supported)
                return t.IsValueType ? t : typeof(object);
            });
        }

        /// <summary>
        /// Returns a strongly-typed converter Func&lt;TSrc, TDest&gt; implementing
        /// the same conversion semantics as SetMemberValue. Compiled & cached.
        /// </summary>
        public static Delegate GetConverterDelegate(Type srcType, Type destType) {
            if (srcType == null) throw new ArgumentNullException(nameof(srcType));
            if (destType == null) throw new ArgumentNullException(nameof(destType));

            // Normalize destination to underlying when Nullable<T>
            var destUnderlying = Nullable.GetUnderlyingType(destType);
            var effectiveDest = destUnderlying ?? destType;

            return _converterCache.GetOrAdd((srcType, effectiveDest), key =>
            {
                var (src, dest) = key;
                return BuildStrongConverter(src, dest);
            });
        }

        // ====== BUILDER ======
        private static Delegate BuildStrongConverter(Type src, Type dest) {
            // Build a Func<TSrc, TDest>
            var funcType = typeof(Func<,>).MakeGenericType(src, dest);
            var p = Expression.Parameter(src, "v");

            Expression body = BuildConversionBody(src, dest, p);

            // If dest is value type, body already of dest; otherwise ensure cast
            var lambda = Expression.Lambda(funcType, body, p);
            return lambda.Compile();
        }

        // Cache for discovered user-defined conversions
        private static readonly ConcurrentDictionary<(Type Src, Type Dest), MethodInfo?> _userOpCache = new();

        private static bool TryGetUserDefinedConversion(Type src, Type dest, out MethodInfo method) {
            method = _userOpCache.GetOrAdd((src, dest), key => FindUserDefinedConversion(key.Src, key.Dest));
            return method != null;
        }

        // Look for op_Implicit/op_Explicit on either side, with return == dest and single parameter assignable from src
        private static MethodInfo? FindUserDefinedConversion(Type src, Type dest) {
            const BindingFlags Flags = BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy;

            // scan both types; operators can be declared on either
            foreach (var t in new[] { src, dest }) {
                foreach (var m in t.GetMethods(Flags)) {
                    if ((m.Name == "op_Implicit" || m.Name == "op_Explicit") &&
                        m.ReturnType == dest) {
                        var ps = m.GetParameters();
                        if (ps.Length == 1 && ps[0].ParameterType.IsAssignableFrom(src))
                            return m;
                    }
                }
            }

            return null;
        }
        private static Expression BuildConversionBody(Type src, Type dest, Expression p) { 
            // Se src for object, construa uma cadeia de conversão em tempo de execução
            if (src == typeof(object)) {
                return BuildObjectRuntimeAwareConversion(p, dest);
            }

            // Identidade
            if (src == dest) return p;

            // (mantenha seus casos especiais existentes)
            if (dest.IsEnum) {
                var u = Enum.GetUnderlyingType(dest);
                Expression toUnderlying = src == u ? p : Expression.Convert(p, u);
                return Expression.Convert(toUnderlying, dest);
            }
            if (dest == typeof(int) && src == typeof(long)) return Expression.Convert(p, typeof(int));
            if (dest == typeof(long) && src == typeof(int)) return Expression.Convert(p, typeof(long));
            if (dest == typeof(DateTime) && src == typeof(DateTimeOffset)) {
                var dtProp = typeof(DateTimeOffset).GetProperty(nameof(DateTimeOffset.DateTime))!;
                return Expression.Property(p, dtProp);
            }
            if (dest == typeof(bool) && src == typeof(sbyte)) return Expression.NotEqual(p, Expression.Constant((sbyte)0));
            if (dest == typeof(ulong) && src == typeof(long)) return Expression.Convert(p, typeof(ulong));
            if (dest == typeof(bool) && src == typeof(string)) {
                var cmp = Expression.Constant(StringComparison.OrdinalIgnoreCase);
                var equals = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string), typeof(StringComparison) })!;
                var isTrue = Expression.Call(p, equals, Expression.Constant("true"), cmp);
                var isYes = Expression.Call(p, equals, Expression.Constant("yes"), cmp);
                var isOne = Expression.Equal(p, Expression.Constant("1"));
                return Expression.OrElse(Expression.OrElse(isTrue, isYes), isOne);
            }
            if (dest == typeof(TimeSpan) && src == typeof(string)) {
                var parse = typeof(TimeSpan).GetMethod(nameof(TimeSpan.Parse), new[] { typeof(string) })!;
                return Expression.Call(parse, p);
            }

            // Atribuível (referência ou boxing)
            if (dest.IsAssignableFrom(src)) 
                return p;

            // >>> CORREÇÃO: conversão definida pelo usuário
            if (TryGetUserDefinedConversion(src, dest, out var op)) {
                // Use Expression.Convert com MethodInfo para vincular o operador
                return Expression.Convert(p, dest, op);
            }

            // fallback IConvertible
            if (typeof(IConvertible).IsAssignableFrom(src) && typeof(IConvertible).IsAssignableFrom(dest)) {
                var changeType = typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!;
                var asObj = Expression.Convert(p, typeof(object));
                var call = Expression.Call(changeType, asObj, Expression.Constant(dest, typeof(Type)));
                return Expression.Convert(call, dest);
            }

            if (dest == typeof(string)) {
                var toString = src.GetMethod(nameof(ToString), Type.EmptyTypes);
                if (toString != null) {
                    return Expression.Call(p, toString);
                }
            }

            // Último recurso: cast explícito (pode lançar se não suportado)
            return Expression.Convert(p, dest);
        }

        private static Expression BuildObjectRuntimeAwareConversion(Expression pObject, Type dest) {
            // null => default(dest)
            var isNull = Expression.Equal(pObject, Expression.Constant(null));
            Expression result = Expression.Default(dest);

            // DBNull.Value => default(dest)
            var dbNullField = typeof(DBNull).GetField(nameof(DBNull.Value))!;
            var dbNullValue = Expression.Field(null, dbNullField);
            var isDbNull = Expression.ReferenceEqual(pObject, dbNullValue);
            result = Expression.Condition(isDbNull, Expression.Default(dest), result);

            // Prepare candidate source types to test at runtime
            var candidates = new List<Type>();
            var effectiveDest = Nullable.GetUnderlyingType(dest) ?? dest;

            // Prefer exact destination type first (handles assignable casts fast)
            candidates.Add(effectiveDest);

            // If enum: try underlying integral and string
            if (effectiveDest.IsEnum) {
                candidates.Add(Enum.GetUnderlyingType(effectiveDest));
                candidates.Add(typeof(string));
            }

            // Add known provider/native candidates
            foreach (var c in _dbSourceCandidates) {
                if (!candidates.Contains(c)) candidates.Add(c);
            }

            // Build cascading "if (v is T) convert((T)v) else ..." chain
            Expression chain = null!;
            foreach (var c in candidates.Distinct()) {
                // if (pObject is c) BuildConversionBody(c -> dest) using (c)pObject
                var asType = Expression.Convert(pObject, c);
                var test = Expression.TypeIs(pObject, c);
                try {
                    var body = BuildConversionBody(c, dest, asType);
                    chain = chain == null ? (Expression)Expression.Condition(test, body, Expression.Default(dest))
                                          : (Expression)Expression.Condition(test, body, chain);
                } catch(Exception x) {
                    // ignore non-sensical conversions
                }
            }

            // If nothing matched and dest == string, fallback to ToString()
            if (effectiveDest == typeof(string)) {
                var toStringCall = Expression.Call(pObject, typeof(object).GetMethod(nameof(ToString))!);
                // chain might be null if no candidates; safeguard
                chain = chain == null ? (Expression)toStringCall : (Expression)Expression.Condition(isNull, Expression.Default(dest), chain);
                // ensure final result is string
                chain = Expression.Condition(Expression.Not(isNull), toStringCall, Expression.Default(dest));
            } else {
                // Generic IConvertible fallback: Convert.ChangeType(pObject, dest)
                var changeType = typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!;
                var convertibleTest = Expression.TypeIs(pObject, typeof(IConvertible));
                var changeTypeCall = Expression.Convert(
                    Expression.Call(changeType, Expression.Convert(pObject, typeof(object)), Expression.Constant(effectiveDest, typeof(Type))),
                    effectiveDest
                );
                var elseCast = Expression.Convert(pObject, effectiveDest); // last resort (may throw)
                var fallback = Expression.Condition(convertibleTest, changeTypeCall, elseCast);

                chain = chain == null ? (Expression)fallback : (Expression)Expression.Condition(isNull, Expression.Default(dest), chain);
                // ensure value type null-safety: when null -> default(dest)
                chain = Expression.Condition(isNull, Expression.Default(dest), chain);
                // Append fallback if previous chain didn't handle the type
                chain = Expression.Condition(
                    Expression.Constant(true), // always reach; chain sub-branches have conditions inside
                    chain,
                    fallback
                );
            }

            // Wrap with DBNull/null guards
            var guarded = Expression.Condition(isDbNull, Expression.Default(dest),
                            Expression.Condition(isNull, Expression.Default(dest), chain));

            return guarded;
        }

        private static readonly ConcurrentDictionary<Type, MethodInfo> _getFieldValueCache = new();

        private static readonly MethodInfo _openGetFieldValue =
            typeof(DbDataReader)
                .GetMethods(BindingFlags.Instance | BindingFlags.Public)
                .Single(m => m.Name == nameof(DbDataReader.GetFieldValue)
                    && m.IsGenericMethodDefinition
                    && m.GetParameters().Length == 1); // (int ordinal)

        public static Func<DbDataReader, T> BuildMaterializer<T>((int ordinal, MemberInfo member, Type targetType, Type typeAtReader)[] bindings)
            where T : new() {
            // Plano:
            // - Criar variável 'vals' como object[] e preencher com r.GetValues(vals)
            // - Para cada binding:
            //   - Ler vals[ordinal]
            //   - Checar DBNull comparando com DBNull.Value
            //   - Converter usando GetConverterDelegate(typeof(object), targetType) quando necessário
            //   - Atribuir respeitando nulabilidade do destino
            var dr = Expression.Parameter(typeof(DbDataReader), "r");
            var obj = Expression.Variable(typeof(T), "obj");
            var vals = Expression.Variable(typeof(object[]), "vals");

            var assignObj = Expression.Assign(obj, Expression.New(typeof(T)));

            var block = new List<Expression> { assignObj };

            // vals = new object[r.FieldCount];
            var fieldCountProp = typeof(DbDataReader).GetProperty(nameof(DbDataReader.FieldCount))!;
            var newArray = Expression.NewArrayBounds(typeof(object), Expression.Property(dr, fieldCountProp));
            var assignVals = Expression.Assign(vals, newArray);
            block.Add(assignVals);

            // r.GetValues(vals);
            var getValuesMethod = typeof(DbDataReader).GetMethod(nameof(IDataRecord.GetValues), new[] { typeof(object[]) })!;
            var callGetValues = Expression.Call(dr, getValuesMethod, vals);
            block.Add(callGetValues);

            foreach (var b in bindings) {
                if (!(b.member is PropertyInfo) && !(b.member is FieldInfo))
                    continue;
                if (b.member is PropertyInfo pi && pi.SetMethod == null)
                    continue;

                if (FiTechCoreExtensions.EnableDebug) {
                    var exprs = DebugModeConvertAndAssign(dr, obj, vals, b);
                    block.AddRange(exprs);
                } else {
                    var exprs = ReleaseModeConvertAndAssign(dr, obj, vals, b);
                    block.AddRange(exprs);
                }
            }

            block.Add(obj);
            var body = Expression.Block(new[] { obj, vals }, block);

            var lambda = Expression.Lambda<Func<DbDataReader, T>>(body, dr);
            return lambda.Compile();
        }

        // Função que gera as expressões para ler, converter e atribuir sem tratamento adicional (modo release)
        private static readonly MethodInfo _dbReaderGetFieldTypeMethod = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFieldType), new[] { typeof(int) })!;
        private static readonly MethodInfo _getConverterDelegateMethod = typeof(ReflectionTool).GetMethod(nameof(GetConverterDelegate), BindingFlags.Public | BindingFlags.Static)!;
        private static readonly MethodInfo _delegateDynamicInvokeMethod = typeof(Delegate).GetMethod(nameof(Delegate.DynamicInvoke), new[] { typeof(object[]) })!;

        private static IEnumerable<Expression> ReleaseModeConvertAndAssign(ParameterExpression dr, ParameterExpression obj, ParameterExpression vals, (int ordinal, MemberInfo member, Type targetType, Type typeAtReader) b) {
            var expressions = new List<Expression>();

            // var value = vals[ordinal];
            var ordinalExpression = Expression.Constant(b.ordinal, typeof(int));
            var value = Expression.ArrayIndex(vals, ordinalExpression);

            // var isDbNull = ReferenceEquals(value, DBNull.Value);
            var dbNullField = typeof(DBNull).GetField(nameof(DBNull.Value))!;
            var dbNullValue = Expression.Field(null, dbNullField);
            var isDbNull = Expression.ReferenceEqual(value, dbNullValue);

            // Convert the incoming reader type to the target member type
            Expression valueExpr;
            var ulTarget = Nullable.GetUnderlyingType(b.targetType) ?? b.targetType;

            var sourceTypeExpr = Expression.Call(dr, _dbReaderGetFieldTypeMethod, ordinalExpression);
            var converterDelegateExpr = Expression.Call(
                _getConverterDelegateMethod,
                sourceTypeExpr,
                Expression.Constant(b.targetType, typeof(Type)));

            valueExpr = Expression.Call(
                converterDelegateExpr,
                _delegateDynamicInvokeMethod,
                Expression.NewArrayInit(typeof(object), value));
            //if (b.targetType == typeof(object)) {
            //    valueExpr = value;
            //} else if (b.typeAtReader == b.targetType || (ulTarget == b.typeAtReader)) {
            //    valueExpr = value;
            //} else if (ulTarget.IsEnum && b.typeAtReader == typeof(Int32)) {
            //    valueExpr = Expression.Convert(value, typeof(Int32));
            //} else if (ulTarget == typeof(Boolean) && b.typeAtReader == typeof(sbyte)) {
            //    valueExpr = Expression.NotEqual(Expression.Convert(value, typeof(sbyte)), Expression.Constant((sbyte)0));
            //} else if (ulTarget == typeof(sbyte) && b.typeAtReader == typeof(Boolean)) {
            //    valueExpr = Expression.Condition(
            //        Expression.Convert(value, typeof(bool)),
            //        Expression.Constant((sbyte)1),
            //        Expression.Constant((sbyte)0));
            //} else if (ulTarget == typeof(UInt64) && b.typeAtReader == typeof(Int64)) {
            //    valueExpr = Expression.Convert(value, typeof(Int64));
            //} else if (ulTarget == typeof(decimal) && b.typeAtReader == typeof(double)) {
            //    valueExpr = Expression.Convert(value, typeof(decimal));
            //} else if (ulTarget == typeof(double) && b.typeAtReader == typeof(decimal)) {
            //    valueExpr = Expression.Convert(value, typeof(double));
            //} else {
            //    valueExpr = Expression.Invoke(
            //            Expression.Constant(GetConverterDelegate(b.typeAtReader, b.targetType)),
            //            Expression.Convert(value, b.typeAtReader));
            //}
            
            //else {
            //    var sourceTypeExpr = Expression.Call(dr, _dbReaderGetFieldTypeMethod, ordinalExpression);
            //    var converterDelegateExpr = Expression.Call(
            //        _getConverterDelegateMethod,
            //        sourceTypeExpr,
            //        Expression.Constant(b.targetType, typeof(Type)));

            //    valueExpr = Expression.Call(
            //        converterDelegateExpr,
            //        _delegateDynamicInvokeMethod,
            //        Expression.NewArrayInit(typeof(object), value));
            //}

            MemberExpression memberExpr = b.member switch {
                PropertyInfo p => Expression.Property(obj, p),
                FieldInfo f => Expression.Field(obj, f),
                _ => throw new NotSupportedException()
            };

            if (!b.targetType.IsValueType || Nullable.GetUnderlyingType(b.targetType) != null) {
                // Nullable ou ref type: atribuir default(null) se DBNull
                var assignableExpr = b.targetType == typeof(object) ? valueExpr : Expression.Convert(valueExpr, b.targetType);
                var conditionalExpr = Expression.Condition(
                    isDbNull,
                    Expression.Default(b.targetType),
                    assignableExpr);

                expressions.Add(Expression.Assign(memberExpr, conditionalExpr));
            } else {
                // Value type não-nullable: só atribui se não for DBNull
                var assign = Expression.Assign(memberExpr, Expression.Convert(valueExpr, b.targetType));
                expressions.Add(Expression.IfThen(Expression.Not(isDbNull), assign));
            }

            return expressions;
        }

        // Versão com try/catch que envolve leitura/conversão/atribuição e lança uma ReflectionException informando o membro que falhou
        private static IEnumerable<Expression> DebugModeConvertAndAssign(ParameterExpression dr, ParameterExpression obj, ParameterExpression vals, (int ordinal, MemberInfo member, Type targetType, Type typeAtReader) b) {
            // Reutiliza a lógica para obter as expressões internas
            var inner = ReleaseModeConvertAndAssign(dr, obj, vals, b).ToArray();

            // Força o bloco try a ser do tipo void para casar com o tipo do catch (void)
            var tryBlock = Expression.Block(typeof(void), inner);

            var exParam = Expression.Parameter(typeof(Exception), "ex");

            string msgText = $"Materialization error binding {b.ordinal} '{b.member.Name}' on '{b.member.DeclaringType?.FullName}'";
            var messageConst = Expression.Constant(msgText);

            // Monta a mensagem: preâmbulo (o valor bruto já está em 'vals', mas não incluímos no texto por simplicidade aqui)
            var concatParamsMethod = typeof(string).GetMethod(nameof(string.Concat), new[] { typeof(object[]) })!;
            var concatArray = Expression.NewArrayInit(typeof(object), messageConst);
            var appendedMessage = Expression.Call(concatParamsMethod, concatArray);

            // Usa o ctor (string, Exception) e passa a exceção capturada como inner exception
            var reflectionExceptionCtor = typeof(ReflectionException).GetConstructor(new[] { typeof(string), typeof(Exception) });
            Expression throwNew;
            if (reflectionExceptionCtor != null) {
                var newEx = Expression.New(reflectionExceptionCtor, appendedMessage, exParam);
                throwNew = Expression.Throw(newEx);
            } else {
                // Fallback: relança a exceção original
                throwNew = Expression.Rethrow();
            }

            var catchClause = Expression.Catch(exParam, throwNew);
            var tryCatch = Expression.TryCatch(tryBlock, catchClause);

            return new[] { tryCatch };
        }

        static ConcurrentDictionary<MemberInfo, Action<object, object>?> _setterMethodCache = new ConcurrentDictionary<MemberInfo, Action<object, object>?>();
        static ConcurrentDictionary<(MemberInfo, Type?), Action<object, object>> _setterConversionCache = new ConcurrentDictionary<(MemberInfo, Type?), Action<object, object>>();
        static void TvDoNothing(object t, object v) { }

        public static void SetMemberValue(MemberInfo member, Object target, Object value) {
            if (_setterConversionCache.TryGetValue((member, value?.GetType()), out var setter)) {
                setter(target, value);
                return;
            }

            var type = GetTypeOf(member);
            if (value != null && value is DBNull) {
                value = null;
                if (type.IsValueType) {
                    _setterConversionCache[(member, value?.GetType())] = TvDoNothing;
                } else {
                    _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, null); _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, null);
                    _setMemberValueInternal(member, target, null);
                }
                return;
            }
            (var isNullable, type) = ToUnderlying(type);
            if (type == null) return;

            if (value == null && (isNullable || !type.IsValueType)) {
                _setterConversionCache[(member, null)] = (t, v) => _setMemberValueInternal(member, t, null);
                _setMemberValueInternal(member, target, null);
                return;
            }
            if (value != null && type == value.GetType()) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, v);
                _setMemberValueInternal(member, target, value);
                return;
            }

            if (value == null) {
                _setterConversionCache[(member, value?.GetType())] = TvDoNothing;
                return;
            }

            var vt = value.GetType();
            if (type == typeof(Int32) && vt == typeof(Int64)) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, (int)(long)v);
                _setMemberValueInternal(member, target, (int)(long)value);
                return;
            }
            if (type == typeof(Int64) && vt == typeof(Int32)) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, (long)(int)v);
                _setMemberValueInternal(member, target, (long)(int)value);
                return;
            }
            if (type == typeof(DateTime) && vt == typeof(DateTimeOffset)) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, ((DateTimeOffset)v).DateTime);
                _setMemberValueInternal(member, target, ((DateTimeOffset)value).DateTime);
                return;
            }

            if (type == typeof(bool) && value is sbyte vb) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, (sbyte)v != 0);
                _setMemberValueInternal(member, target, vb != 0);
                return;
            }
            if (type.IsEnum && value is int nval) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, Enum.ToObject(type, (int)v));
                value = Enum.ToObject(type, nval);
                _setMemberValueInternal(member, target, value);
                return;
            } else if (type.IsEnum && value is long lval) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, Enum.ToObject(type, (int)(long)v));
                value = Enum.ToObject(type, (int)lval);
                _setMemberValueInternal(member, target, value);
                return;
            }

            if (value == null) {
                if (type.IsValueType || !isNullable) {
                    _setterConversionCache[(member, value?.GetType())] = TvDoNothing;
                    return;
                } else {
                    _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, null);
                    _setMemberValueInternal(member, target, null);
                    return;
                }
            }

            if (value is long l && type == typeof(UInt64)) {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, (ulong)l);
                _setMemberValueInternal(member, target, (ulong)l);
                return;
            }

            if (value.GetType() != type) {
                if (type.IsGenericType) {
                    if (type.GetGenericTypeDefinition() == typeof(FnVal<>)) {
                        target = GetMemberValue(member, target);
                        member = type.GetProperty("Value");
                        type = type.GetGenericArguments()[0];
                    }
                } else {
                    if (value is string str && type == typeof(bool)) {
                        _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, str.ToLower() == "true" || str.ToLower() == "yes" || str == "1");
                        value = str.ToLower() == "true" || str.ToLower() == "yes" || str == "1";
                        _setMemberValueInternal(member, target, value);
                        return;
                    }

                    if (type.IsAssignableFrom(value.GetType())) {
                        _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, v);
                        _setMemberValueInternal(member, target, value);
                        return;
                    }

                    var castMethod = CastMethods[type][value.GetType()];
                    if (castMethod != null) {
                        _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, castMethod.Invoke(t, new object[] { v }));
                        value = castMethod.Invoke(target, new object[] { value });
                        _setMemberValueInternal(member, target, value);
                        return;
                    }

                    if (value.GetType().Implements(typeof(IConvertible))) {
                        try {
                            _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, Convert.ChangeType(v, type));
                            value = Convert.ChangeType(value, type);
                            _setMemberValueInternal(member, target, value);
                            return;
                        } catch (Exception ex) {
                            if (StrictMode) {
                                if (Debugger.IsAttached) {
                                    Debugger.Break();
                                }
                                throw new ReflectionException($"Error casting {value} from {value.GetType().Name} to {type.Name} for field {target.GetType().Name}{member.Name}");
                            }
                        }
                    }
                }

                if (type.FullName == "System.TimeSpan" && value is string strTs) {
                    _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, TimeSpan.Parse((string)v));
                    value = TimeSpan.Parse(strTs);
                    _setMemberValueInternal(member, target, value);
                    return;
                }
            } else {
                _setterConversionCache[(member, value?.GetType())] = (t, v) => _setMemberValueInternal(member, t, v);
                _setMemberValueInternal(member, target, value);
                return;
            }

            _setterConversionCache[(member, value?.GetType())] = TvDoNothing;

            if (StrictMode) {
                if (Debugger.IsAttached) {
                    Debugger.Break();
                }

                throw new ReflectionException($"Reflection Tool could not set {value} ({value?.GetType()}) into {member.DeclaringType.Name}::{member.Name} ({ReflectionTool.GetTypeOf(member)})");
            }
            // _setMemberValueInternal(pi, fi, member, target, value); 
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
            if (target == null) {
                return null;
            }

            var retv = GetMemberValue(GetMember(target.GetType(), fieldName), target);
            return retv;
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
            if (key == null || type == null) {
                Debugger.Break();
                return false;
            }
            return MemberCacheFromString[type].ContainsKey(key);
        }
    }
}
