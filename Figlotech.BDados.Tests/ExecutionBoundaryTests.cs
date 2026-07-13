using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Core;
using Figlotech.Core.Interfaces;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class ExecutionBoundaryTests {
        [Fact]
        public void RdbmsDataAccessorHasNoLegacyAutoCacheFields() {
            Type type = typeof(RdbmsDataAccessor);
            string[] forbiddenNames = new[] {
                "CacheAutoJoin",
                "CacheAutoLinear",
                "CacheAutoPrefixer",
                "CacheBuildParams",
                "CacheAutoJoinDict",
                "CacheAutoBuildDict",
                "_autoAggregateCache"
            };

            foreach (string name in forbiddenNames) {
                FieldInfo? field = type.GetField(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance);
                Assert.Null(field);
            }
        }

        [Fact]
        public void RdbmsDataAccessorHasNoLegacyTraversalMethods() {
            Type type = typeof(RdbmsDataAccessor);
            string[] forbiddenNames = new[] {
                "MakeQueryAggregations",
                "MakeBuildAggregations",
                "GetAutoJoin",
                "GetAutoBuild",
                "CreateFieldNamesDict"
            };

            foreach (string name in forbiddenNames) {
                MethodInfo? method = type.GetMethod(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance);
                Assert.Null(method);
            }
        }

        [Fact]
        public void CompilerHasNoCreateLegacySnapshotMethod() {
            Type type = typeof(DefinitiveJoinPlanCompiler);
            MethodInfo? method = type.GetMethod("CreateLegacySnapshot", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance);
            Assert.Null(method);
        }

        [Fact]
        public void BDadosAssemblyHasNoMutableAggregatePlanTypes() {
            Assembly assembly = typeof(RdbmsDataAccessor).Assembly;
            string[] forbiddenNames = new[] {
                "Figlotech.BDados.DataAccessAbstractions.AggregateRelationPlan",
                "Figlotech.BDados.DataAccessAbstractions.AggregateConstructionContext",
                "Figlotech.BDados.DataAccessAbstractions.AggregateMaterializerPlan"
            };

            foreach (string name in forbiddenNames) {
                Type? forbiddenType = assembly.GetType(name);
                Assert.Null(forbiddenType);
            }
        }

        [Fact]
        public void JoinDefinitionBuilderAdaptersAreObsoleteWithFreezeMessage() {
            Type type = typeof(RdbmsDataAccessor);
            var methods = type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)
                .Where(m => m.GetParameters().Any(p => p.ParameterType == typeof(JoinDefinition)));

            Assert.Equal(3, methods.Count());

            foreach (MethodInfo method in methods) {
                ObsoleteAttribute? attribute = method.GetCustomAttribute<ObsoleteAttribute>();
                Assert.NotNull(attribute);
                Assert.Contains("Freeze", attribute!.Message, StringComparison.OrdinalIgnoreCase);
                Assert.Contains("DefinitiveJoinPlan", attribute.Message, StringComparison.OrdinalIgnoreCase);
            }
        }

        [Fact]
        public void FrozenJoinDefinitionOverloadCounterpartsRemain() {
            Type type = typeof(RdbmsDataAccessor);
            var joinDefinitionMethods = type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)
                .Where(m => m.GetParameters().Any(p => p.ParameterType == typeof(JoinDefinition)))
                .ToList();

            foreach (MethodInfo legacyMethod in joinDefinitionMethods) {
                string name = legacyMethod.Name;
                var counterpart = type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)
                    .Where(m => m.Name == name && m.GetParameters().Any(p => p.ParameterType == typeof(DefinitiveJoinPlan)))
                    .ToList();
                Assert.NotEmpty(counterpart);
            }
        }

        [Fact]
        public void BDadosAssemblyHasNoJoinDefinitionKeyedGenericCaches() {
            Assembly assembly = typeof(RdbmsDataAccessor).Assembly;
            Type[] cacheTypes = new[] {
                typeof(Dictionary<,>),
                typeof(System.Collections.Concurrent.ConcurrentDictionary<,>),
                typeof(System.Runtime.CompilerServices.ConditionalWeakTable<,>),
                typeof(SelfInitializerDictionary<,>)
            };

            IReadOnlyList<FieldInfo> fields = GetAllFields(assembly.GetTypes());
            foreach (FieldInfo field in fields) {
                Type fieldType = field.FieldType;
                if (!fieldType.IsGenericType)
                    continue;

                Type genericDefinition = fieldType.GetGenericTypeDefinition();
                if (!cacheTypes.Contains(genericDefinition))
                    continue;

                Assert.False(
                    fieldType.GetGenericArguments().Any(ContainsJoinDefinition),
                    $"Field {field.DeclaringType!.Name}.{field.Name} has {genericDefinition.Name} containing JoinDefinition");
            }
        }

        [Fact]
        public void StateUpdateMaterializationPreservesConversionAndDbNullHandling() {
            using var accessor = new RdbmsDataAccessor(new SqlitePlugin(new SqlitePluginConfiguration {
                DataSource = ":memory:",
                Schema = "main"
            }));
            var table = new DataTable();
            table.Columns.Add("TypeName", typeof(string));
            table.Columns.Add("data_0", typeof(string));
            table.Columns.Add("data_1", typeof(object));
            table.Rows.Add(nameof(StateUpdateRow), "42", DBNull.Value);

            using DataTableReader reader = table.CreateDataReader();
            Type rowType = typeof(StateUpdateRow);
            var fields = new Dictionary<Type, MemberInfo[]> {
                [rowType] = new[] {
                    rowType.GetProperty(nameof(StateUpdateRow.Id))!,
                    rowType.GetProperty(nameof(StateUpdateRow.NonNullableValue))!
                }
            };
            MethodInfo materialize = typeof(RdbmsDataAccessor).GetMethod(
                "BuildStateUpdateQueryResult",
                BindingFlags.NonPublic | BindingFlags.Instance)!;

            var result = (List<ILegacyDataObject>)materialize.Invoke(
                accessor,
                new object?[] { null, reader, new List<Type> { rowType }, fields })!;

            StateUpdateRow row = Assert.IsType<StateUpdateRow>(Assert.Single(result));
            Assert.Equal(42, row.Id);
            Assert.Equal(17, row.NonNullableValue);
        }

        private static bool ContainsJoinDefinition(Type type) {
            return type == typeof(JoinDefinition)
                || type.IsGenericType && type.GetGenericArguments().Any(ContainsJoinDefinition);
        }

        private static IReadOnlyList<FieldInfo> GetAllFields(IEnumerable<Type> types) {
            var result = new List<FieldInfo>();
            foreach (Type type in types.Where(t => t.IsClass || t.IsValueType)) {
                try {
                    result.AddRange(type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance));
                } catch {
                    // Skip types that cannot be inspected.
                }
            }
            return result;
        }

        private sealed class StateUpdateRow : ILegacyDataObject {
            object IDataObject.Id {
                get => Id;
                set => Id = Convert.ToInt64(value);
            }

            public long Id { get; set; }
            public int NonNullableValue { get; set; } = 17;
            public string RID { get; set; } = string.Empty;
            public bool IsPersisted { get; set; }
            public int PersistedHash { get; set; }
            public ulong AlteredBy { get; set; }
            public ulong CreatedBy { get; set; }
            public bool IsReceivedFromSync { get; set; }
            public DateTime CreatedAt { get; set; }
            public DateTime? UpdatedAt { get; set; }
        }
    }
}
