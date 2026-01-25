using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Extensions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class Settings : Dictionary<String, Object> { }

    /// <summary>
    /// This is an utilitarian class, it provides quick static logic 
    /// for the rest of BDados to operate
    /// </summary>
    /// 
    public static partial class FiTechBDadosExtensions {
        //        private static Object _readLock = new Object();
        //        private static int _generalId = 0;
        //        public static ILogger ApiLogger;

        //        public static bool EnableStdoutLogs { get; set; } = false;


        public static MemberInfo GetUpdateColumn(this Fi _selfie, Type type) {
            var fields = ReflectionTool.FieldsAndPropertiesOf(type);
            var retv = fields
                .Where((f) => f.GetCustomAttribute<UpdateTimeStampAttribute>() != null)
                .FirstOrDefault();
            return retv;
        }

        public static MemberInfo GetRidColumn<T>(this Fi _selfie) where T : IDataObject { return Fi.Tech.GetRidColumn(typeof(T)); }
        public static MemberInfo GetRidColumn(this Fi _selfie, Type type) {
            var fields = ReflectionTool.FieldsAndPropertiesOf(type);

            var reliable = fields
                .Where((f) => f.GetCustomAttribute<ReliableIdAttribute>() != null)
                .FirstOrDefault();
            if (reliable != null) {
                return reliable;
            }

            var primaryKey = fields
                .Where((f) => f.GetCustomAttribute<PrimaryKeyAttribute>() != null)
                .FirstOrDefault();
            return primaryKey;
        }
        public static SelfInitializerDictionary<Type, string> UpdateColumnNameOf { get; private set; } = new SelfInitializerDictionary<Type, string>((t) => {
            var explicitColumn = Fi.Tech.GetUpdateColumn(t)?.Name;
            if (explicitColumn != null) {
                return explicitColumn;
            }
            return typeof(ILegacyDataObject).IsAssignableFrom(t) ? "UpdatedTime" : "UpdatedAt";
        });
        public static SelfInitializerDictionary<Type, MemberInfo> UpdateColumnOf { get; private set; } = new SelfInitializerDictionary<Type, MemberInfo>((t) => Fi.Tech.GetUpdateColumn(t));
        
        public static SelfInitializerDictionary<Type, String> RidColumnNameOf { get; private set; } = new SelfInitializerDictionary<Type, string>((t) => Fi.Tech.GetRidColumn(t)?.Name ?? "RID");
        public static SelfInitializerDictionary<Type, MemberInfo> RidColumnOf { get; private set; } = new SelfInitializerDictionary<Type, MemberInfo>((t) => Fi.Tech.GetRidColumn(t));

        public static SelfInitializerDictionary<Type, String> IdColumnNameOf { get; private set; } = new SelfInitializerDictionary<Type, string>((t) => Fi.Tech.GetIdColumn(t)?.Name ?? "Id");
        public static SelfInitializerDictionary<Type, MemberInfo> IdColumnOf { get; private set; } = new SelfInitializerDictionary<Type, MemberInfo>((t) => Fi.Tech.GetIdColumn(t));

        public static MemberInfo GetIdColumn<T>(this Fi _selfie) where T : IDataObject, new() { return Fi.Tech.GetIdColumn(typeof(T)); }
        public static MemberInfo GetIdColumn(this Fi _selfie, Type type) {
            var fields = ReflectionTool.FieldsAndPropertiesOf(type);
            var retv = fields
                .Where((f) => f.GetCustomAttribute<PrimaryKeyAttribute>() != null)
                .FirstOrDefault();
            return retv;
        }

        //        private static Lazy<WorkQueuer> _globalQueuer = new Lazy<WorkQueuer>(()=> new WorkQueuer("FIGLOTECH_GLOBAL_QUEUER", Environment.ProcessorCount, true));
        //        public static WorkQueuer GlobalQueuer { get => _globalQueuer.Value; }

        //        public static int currentBDadosConnections = 0;

        //        public static List<string> RanChecks = new List<string>();

        //        public static string DefaultLogRepository = "Logs\\Fi.TechLogs";

        //        public static String DefaultBackupStore { get; set; } = "../Backups/";

        //        public static String Version {
        //            get {
        //                return Assembly.GetExecutingAssembly().GetName().Version.ToString();
        //            }
        //        }

        //        public static void As<T>(this Fi _selfie, object input, Action<T> act) {
        //            if (
        //                (typeof(T).IsInterface && input.GetType().GetInterfaces().Contains(typeof(T))) ||
        //                input.GetType().IsAssignableFrom(typeof(T))
        //            ) {
        //                act((T)input);
        //            }

        //        }

        //        public static void RecursiveGiveRids(this Fi _selfie, IDataObject obj) {
        //            if(obj.RID == null) {
        //                obj.RID = Fi.Tech.GenerateIdString(obj.GetType().Name, 64);
        //                var props = ReflectionTool.FieldsAndPropertiesOf(obj.GetType());

        //                for(int i = 0; i < props.Count; i++) {
        //                    var t = ReflectionTool.GetTypeOf(props[i]);
        //                    if (t.GetInterfaces().Contains(typeof(IDataObject))) {
        //                        Fi.Tech.RecursiveGiveRids((IDataObject) ReflectionTool.GetValue(obj, props[i].Name));
        //                    }
        //                }
        //            }
        //        }

        //        public static T Deserialize<T>(this Fi _selfie, String txt) where T: IDataObject, new() {
        //            var v = JsonConvert.DeserializeObject<T>(txt);
        //            Fi.Tech.RecursiveGiveRids(v);
        //            return v;
        //        }

        /// <summary>
        /// deprecated
        /// this gimmick should barely be used by the data accessors
        /// provided by the rdbms language providers
        /// But it's a contravention, must be avoided whenever possible.
        /// </summary>
        /// <param name="valor"></param>
        /// <returns></returns>
        public static IEnumerable<T> MapFromReader<T>(this Fi _selfie, IDataReader reader, bool ignoreCase = false) where T : new() {
            var materializer = FiTechBDadosExtensions.GetSimpleLoadAllMaterializerFor<T>(reader);
            while (reader.Read()) {
                T obj = materializer(reader as DbDataReader);
                yield return obj;
            }
            yield break;
        }

        static ConcurrentDictionary<(Type, string), object> MaterializerCache = new ConcurrentDictionary<(Type, string), object>();
        public static Func<DbDataReader, T> GetSimpleLoadAllMaterializerFor<T>(IDataReader reader) where T : new() {
            var fieldNames = Fi.Range(0, reader.FieldCount).Select(i => reader.GetName(i)).ToArray();
            var tag = String.Join(";", fieldNames) + $"|{typeof(T).FullName}";
            return (Func<DbDataReader, T>)MaterializerCache.GetOrAddWithLocking((typeof(T), tag), t => {
                var cols = new string[reader.FieldCount];
                for (int i = 0; i < cols.Length; i++)
                    cols[i] = reader.GetName(i);
                                
                var existingKeys = new (MemberInfo Member, int Ordinal, Type TypeAtReader)[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++) {
                    var name = cols[i];
                    if (name != null) {
                        var m = ReflectionTool.GetMember(typeof(T), name);
                        if (m != null) {
                            var ord = reader.GetOrdinal(name);
                            existingKeys[i] = (m, ord, reader.GetFieldType(ord));
                        }
                    }
                }
                int c = 0;
                var swBuild = Stopwatch.StartNew();

                var materializer = ReflectionTool.BuildMaterializer<T>(
                    existingKeys
                        .Where(x => x.Member != null)
                        .Select((tuple, i) => (tuple.Ordinal, tuple.Member, ReflectionTool.GetTypeOf(tuple.Member), tuple.TypeAtReader))
                        .ToArray()
                );

                return materializer;
            });
        }

        public static async IAsyncEnumerable<T> MapFromReaderAsync<T>(this Fi _selfie, DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken, bool ignoreCase = false) where T : new() {

            var materializer = FiTechBDadosExtensions.GetSimpleLoadAllMaterializerFor<T>(reader);
            
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) {
                T obj = materializer(reader);
                yield return obj;
            }
            yield break;
        }

        public static String CheapSanitize(this Fi _selfie, Object valor) {
            String valOutput;
            if (valor == null)
                return "NULL";
            if (valor.GetType().IsEnum) {
                return $"{(int)Convert.ChangeType(valor, Enum.GetUnderlyingType(valor.GetType()))}";
            }
            switch (valor.GetType().Name.ToLower()) {
                case "string":
                    if (valor.ToString() == "CURRENT_TIMESTAMP")
                        return "CURRENT_TIMESTAMP";
                    var invalid = new char[] { (char) 0x0, '\n', '\r', '\\', '\'', (char) 0x1a };
                    valOutput = String.Join(string.Empty, (valor as string).Select(a=>
                        invalid.Contains(a) ? $"\\{a}" : $"{a}"
                    ));
                    return $"'{valOutput}'";
                case "float":
                case "double":
                case "decimal":
                    valOutput = Convert.ToString(valor).Replace(",", ".");
                    return $"{valOutput}";
                case "short":
                case "int":
                case "long":
                case "int16":
                case "int32":
                case "int64":
                    return Convert.ToString(valor);
                case "datetime":
                    return $"'{((DateTime)valor).ToString("yyyy-MM-dd HH:mm:ss")}'";
                default:
                    return Convert.ToString(valor);
            }

        }

        public static void SetPageSizeOfRecordSet(this Fi _selfie, Object o, int? val) {
            try {
                o.GetType().GetMethod("Limit").Invoke(o, new object[] { val });
            } catch (Exception x) {
                Fi.Tech.WriteLine("Error in SetPageSizeOfRecordSet" + x.Message);
            }
        }

        public static int GetCountOfRecordSet(this Fi _selfie, Object o) {
            try {
                return (int)o.GetType().GetProperties().Where((p) => p.Name == "Count").FirstOrDefault().GetValue(o);
            } catch (Exception x) {
                Fi.Tech.WriteLine("Error in GetCountOfRecordSet" + x.Message);
            }
            return -1;
        }
        public static void SetAccessorOfRecordSet(this Fi _selfie, Object o, IDataAccessor da) {
            try {
                o.GetType().GetProperties().Where((p) => p.Name == "DataAccessor").FirstOrDefault().SetValue(o, da);
            } catch (Exception x) {
                Fi.Tech.WriteLine("Error in SetAccessorOfRecordSet" + x.Message);
            }
        }

        public static Object FindOfType(this Fi _selfie, IDataAccessor da, Type o, int p, int? limit) {
            try {
                var type = da
                        .GetType();
                var methods = type
                        .GetMethods();
                var method = methods
                        .Where(
                            (m) => m.Name == "Find" && m.GetParameters().Count() == 3
                            && m.GetParameters()[1].ParameterType == typeof(int?) && m.GetParameters()[2].ParameterType == typeof(int?))
                        .FirstOrDefault();
                var finalMethod =
                    method
                        .MakeGenericMethod(o);
                return finalMethod?.Invoke(da, new object[] { null, p, limit });
                //o.GetType().GetMethod("Find").Invoke(o, new object[] { null, p });
            } catch (Exception x) {
                Fi.Tech.WriteLine("Error in FindOfType" + x.Message);
            }
            return null;
        }

        public static RecordSet<T> FindOfByUpdateTime<T>(IDataAccessor da, long lastUpdate, int p, int? limit) where T : IDataObject, new() {
            return new RecordSet<T>(da);
        }

        public static Object FindOfByUpdateTime(this Fi _selfie, Type type, IDataAccessor da, long lastUpdate, int p, int? limit) {
            // Holy mother of Reflection!
            var updateTime = new DateTime(lastUpdate);
            var paramExpr = Expression.Parameter(type, "x");
            var member = Expression.MakeMemberAccess(paramExpr, type.GetMembers().FirstOrDefault(m => m.Name == "DataUpdate"));
            var conditionBody = Expression.GreaterThan(member, Expression.Convert(Expression.Constant(updateTime), member.Type));
            var funcTBool = typeof(Func<,>).MakeGenericType(
                        type, typeof(bool));
            var exprMethod = typeof(Expression)
                .GetMethods().FirstOrDefault(
                    m => m.Name == "Lambda")
                .MakeGenericMethod(
                    funcTBool);
            var expr = exprMethod
                .Invoke(null, new object[] { conditionBody, new ParameterExpression[] { paramExpr } });
            var condition = Activator.CreateInstance(
                typeof(Conditions<>).MakeGenericType(type)
                , new object[] { expr });

            var FindMethod = da.GetType().GetMethods().FirstOrDefault(
                m =>
                    m.Name == "Find" &&
                    m.GetParameters().Length == 3)
                .MakeGenericMethod(type);
            var retv = FindMethod
                .Invoke(da, new object[] { expr, 1, null });
            // IT FUCKING WORKS BIATCH YAY!
            return retv;
        }

        public static void SaveRecordSet(this Fi _selfie, Object o) {
            try {
                o.GetType().GetMethod("Save").Invoke(o, new object[1]);
            } catch (Exception x) {
                Fi.Tech.WriteLine("Error in SaveRecordSet" + x.Message);
            }
        }
        //        public sealed class CopyProgressReport {
        //            int min = 0;
        //            int max = 1;
        //            int current = 0;
        //        }
        public static SelfInitializerDictionary<Type, Type> RidFieldType { get; private set; } = new SelfInitializerDictionary<Type, Type>(
            t => ReflectionTool.GetTypeOf(ReflectionTool.FieldsAndPropertiesOf(t).FirstOrDefault(x => x.GetCustomAttribute<ReliableIdAttribute>() != null))
        );

        static int gid = 0;
        public static QueryBuilder ListRids<T>(this Fi _selfie, List<T> set) where T : IDataObject {
            QueryBuilder retv = new QueryBuilder();

            int x = 0;
            var ridType = RidFieldType[typeof(T)];

            int ggid = ++gid;
            for (int i = 0; i < set.Count; i++) {
                retv.Append(
                    new QueryBuilder().Append(
                        $"@r_{i}",
                        ReflectionTool.GetMemberValue(FiTechBDadosExtensions.RidColumnOf[typeof(T)], set[i])
                    )
                );
                if (i < set.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }

        public static QueryBuilder ListRids2<T>(this Fi _selfie, List<T> set) where T : IDataObject {
            QueryBuilder retv = new QueryBuilder();
            for (int i = 0; i < set.Count; i++) {
                retv.Append(
                    new QueryBuilder().Append(
                        set[i].RID
                    )
                );
                if (i < set.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }
        static uint upseq = 0;
        public static QueryBuilder ListIds<T>(this Fi _selfie, RecordSet<T> set) where T : IDataObject, new() {
            QueryBuilder retv = new QueryBuilder();
            uint seq = 0;
            for (int i = 0; i < set.Count; i++) {
                retv.Append(
                    new QbFmt(
                        $"@{++upseq}_{++seq}",
                        set[i].Id
                    )
                );
                if (i < set.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }

        static SelfInitializerDictionary<Type, (MemberInfo, FieldAttribute)[]> FieldMembers = new SelfInitializerDictionary<Type, (MemberInfo, FieldAttribute)[]>(t => {
            return ReflectionTool.FieldsAndPropertiesOf(t).Select(fp => {
                return (fp, fp.GetCustomAttribute<FieldAttribute>());
            })
            .Where(tup=> tup.Item2 != null)
            .ToArray();
        });
        public static String[] GetFieldNames(this Fi _selfie, Type t) {
            var members = FieldMembers[t];
            List<String> retv = new List<String>();
            foreach (var a in members) {
                retv.Add(a.Item1.Name);
            }
            return retv.ToArray();
        }

        public static bool FindColumn(this Fi _selfie, string ChaveA, Type type) {
            var members = FieldMembers[type];
            return members.Any((f) => f.Item1.Name == ChaveA);
        }

        const int keySize = 16;

    }
}
