using System;
using System.Collections.Generic;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions;

namespace Figlotech.BDados.Benchmarks {
    public sealed class BenchmarkMaterializationScenario {
        private readonly Func<int> _materialize;

        internal BenchmarkMaterializationScenario(DefinitiveJoinPlan plan, object[][] rows, Func<int> materialize) {
            Plan = plan;
            Rows = rows;
            _materialize = materialize;
        }

        public DefinitiveJoinPlan Plan { get; }
        public object[][] Rows { get; }

        public int Materialize() {
            return _materialize();
        }
    }

    public static class BenchmarkScenarioFactory {
        public const int RowsPerInvocation = 256;

        public static BenchmarkMaterializationScenario Create(BenchmarkIdentifierKind identifierKind, int columnCount, int nullPercent) {
            if (columnCount != 1 && columnCount != 8 && columnCount != 32 && columnCount != 128) {
                throw new ArgumentOutOfRangeException(nameof(columnCount));
            }
            if (nullPercent != 0 && nullPercent != 10 && nullPercent != 50 && nullPercent != 100) {
                throw new ArgumentOutOfRangeException(nameof(nullPercent));
            }

            return identifierKind switch {
                BenchmarkIdentifierKind.Int32 => CreateTyped<Int32BenchmarkRow>(columnCount, nullPercent, row => row),
                BenchmarkIdentifierKind.Int64 => CreateTyped<Int64BenchmarkRow>(columnCount, nullPercent, row => (long)row),
                BenchmarkIdentifierKind.Guid => CreateTyped<GuidBenchmarkRow>(columnCount, nullPercent, row => CreateGuid(row)),
                BenchmarkIdentifierKind.String => CreateTyped<StringBenchmarkRow>(columnCount, nullPercent, row => "id-" + row.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)),
                _ => throw new ArgumentOutOfRangeException(nameof(identifierKind))
            };
        }

        public static Type GetCompileRootType(CompileGraphSize graphSize) {
            return graphSize switch {
                CompileGraphSize.Small => typeof(SmallCompileRoot),
                CompileGraphSize.Medium => typeof(MediumCompileRoot),
                CompileGraphSize.Large => typeof(LargeCompileRoot),
                _ => throw new ArgumentOutOfRangeException(nameof(graphSize))
            };
        }

        private static BenchmarkMaterializationScenario CreateTyped<TRow>(int columnCount, int nullPercent, Func<int, object> createIdentifier)
            where TRow : class, new() {
            Type rowType = typeof(TRow);
            MemberInfo identifierMember = rowType.GetProperty("Id")
                ?? throw new InvalidOperationException($"Benchmark row type '{rowType.FullName}' has no Id property.");
            MemberInfo valueMember = rowType.GetProperty("Value")
                ?? throw new InvalidOperationException($"Benchmark row type '{rowType.FullName}' has no Value property.");

            var projectedColumns = new List<string>(columnCount);
            var projection = new List<DefinitiveProjectionColumn>(columnCount);
            for (int ordinal = 0; ordinal < columnCount; ordinal++) {
                string sourceColumn = ordinal == 0 ? "Id" : "Value" + ordinal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                string resultAlias = "r_" + sourceColumn;
                projectedColumns.Add(sourceColumn);
                projection.Add(new DefinitiveProjectionColumn(
                    ordinal,
                    0,
                    sourceColumn,
                    resultAlias,
                    ordinal == 0 ? identifierMember : valueMember));
            }

            var identifier = new DefinitiveIdentifier(identifierMember, "Id", 0, "r_Id");
            var table = new DefinitiveJoinTable(
                rowType,
                rowType.Name,
                "root",
                "r",
                JoinType.LEFT,
                null,
                identifier,
                projectedColumns);
            var plan = new DefinitiveJoinPlan(
                rowType,
                AggregateJoinShape.ScalarAggregatesOnly,
                0,
                new[] { table },
                Array.Empty<DefinitiveJoinRelation>(),
                projection,
                new[] { new KeyValuePair<AggregatePath, string>(new AggregatePath(Array.Empty<string>()), "root") },
                new[] { new KeyValuePair<string, int>("root", 0) },
                new RootOrderingRequirement(0, "Id", 0, "r_Id"),
                DefinitiveJoinPlanCompiler.CurrentFormatVersion);

            object[][] rows = CreateRows(columnCount, nullPercent, createIdentifier);
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            return new BenchmarkMaterializationScenario(
                plan,
                rows,
                () => materializer.Materialize<TRow>(rows).Length);
        }

        private static object[][] CreateRows(int columnCount, int nullPercent, Func<int, object> createIdentifier) {
            var rows = new object[RowsPerInvocation][];
            int nullableCell = 0;
            for (int rowIndex = 0; rowIndex < rows.Length; rowIndex++) {
                var row = new object[columnCount];
                row[0] = createIdentifier(rowIndex + 1);
                for (int ordinal = 1; ordinal < columnCount; ordinal++) {
                    bool isNull = nullPercent != 0 && nullableCell % 100 < nullPercent;
                    row[ordinal] = isNull ? DBNull.Value : rowIndex + ordinal;
                    nullableCell++;
                }
                rows[rowIndex] = row;
            }
            return rows;
        }

        private static Guid CreateGuid(int value) {
            byte[] bytes = new byte[16];
            BitConverter.GetBytes(value).CopyTo(bytes, 0);
            return new Guid(bytes);
        }
    }
}
