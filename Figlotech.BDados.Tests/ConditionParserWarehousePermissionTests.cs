using System;
using System.Collections.Generic;
using System.Linq;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Xunit;
using Xunit.Abstractions;

namespace Figlotech.BDados.Tests {
    public class ConditionParserWarehousePermissionTests {
        private readonly ITestOutputHelper _output;

        public ConditionParserWarehousePermissionTests(ITestOutputHelper output) {
            _output = output;
        }

        public sealed class Shift : PlanDataObject<Guid> {
            [Field]
            public string UnidadeArmazenagem { get; set; } = "";

            [AggregateObject(nameof(UnidadeArmazenagem))]
            public Warehouse? Warehouse { get; set; }
        }

        public sealed class Warehouse : PlanDataObject<string> {
            // A joined column with the same name makes a missing root prefix ambiguous.
            [Field]
            public string UnidadeArmazenagem { get; set; } = "";
        }

        private sealed class Permission {
            public string RID { get; set; } = "";
        }

        [Theory]
        [MemberData(nameof(ProviderDefinitiveJoinQueryTests.Generators), MemberType = typeof(ProviderDefinitiveJoinQueryTests))]
        public void ComposedPermissionFilterQualifiesRootColumnAndBindsSelectedRids(string providerName, IQueryGenerator generator) {
            var allowed = new List<Permission> {
                new Permission { RID = "warehouse-a" },
                new Permission { RID = "warehouse-b" }
            };
            var conditions = new Conditions<Shift>(fmc => fmc.Id != Guid.Empty);
            conditions.And(fmc => Qh.In(fmc, "UnidadeArmazenagem", allowed, x => x.RID));
            var plan = AutomaticJoinPlanCache.GetOrAdd(typeof(Shift), AggregateJoinShape.FullGraph);
            string rootAlias = plan.AliasByPath[new AggregatePath(Array.Empty<string>())];
            var parser = new ConditionParser(plan);
            var full = parser.ParseExpression(conditions);
            var root = parser.ParseExpression(conditions.ToLambdaExpression(), fullConditions: false);
            var equality = parser.ParseExpression<Shift>(fmc => fmc.UnidadeArmazenagem == allowed[0].RID);

            Assert.Contains(rootAlias + ".UnidadeArmazenagem", equality.GetCommandText());
            Assert.Contains(rootAlias + ".UnidadeArmazenagem IN", full.GetCommandText());
            Assert.DoesNotContain(rootAlias + ".", root.GetCommandText());
            Assert.Contains("UnidadeArmazenagem IN", root.GetCommandText());
            foreach (var parsed in new[] {
                new ConditionParser().ParseExpression(conditions),
                parser.ParseExpression(conditions.ToLambdaExpression())
            }) {
                Assert.Contains(rootAlias + ".UnidadeArmazenagem IN", parsed.GetCommandText());
                Assert.Equal(full.GetParameters().Values, parsed.GetParameters().Values);
            }
            Assert.Equal(allowed.Select(x => x.RID), full.GetParameters().Values.OfType<string>());
            Assert.Equal(3, full.GetParameters().Count);

            var query = generator.GenerateJoinQuery(plan, full, rootConditions: root);
            string sql = query.GetCommandText();
            Assert.Contains(rootAlias + ".UnidadeArmazenagem IN", sql);
            Assert.Equal(full.GetParameters().Count + root.GetParameters().Count, query.GetParameters().Count);
            Assert.All(query.GetParameters().Keys, key => Assert.Contains("@" + key, sql));
            Assert.DoesNotContain("warehouse-a", sql);
            _output.WriteLine(providerName + ": " + sql);
        }
    }
}
