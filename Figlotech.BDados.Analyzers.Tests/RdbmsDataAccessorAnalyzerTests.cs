using Figlotech.BDados.DataAccessAbstractions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using Microsoft.CodeAnalysis.Testing.Verifiers;
using System.Threading.Tasks;
using Xunit;

namespace Figlotech.BDados.Analyzers.Tests {
    public class RdbmsDataAccessorAnalyzerTests {
        private static CSharpAnalyzerTest<RdbmsDataAccessorAnalyzer, XUnitVerifier> CreateTest(string source, params DiagnosticResult[] expectedDiagnostics) {
            var test = new CSharpAnalyzerTest<RdbmsDataAccessorAnalyzer, XUnitVerifier> {
                TestCode = source,
                ReferenceAssemblies = ReferenceAssemblies.Net.Net60,
            };
            test.TestState.AdditionalReferences.Add(typeof(RdbmsDataAccessor).Assembly);
            foreach (var diag in expectedDiagnostics) {
                test.ExpectedDiagnostics.Add(diag);
            }
            return test;
        }

        [Fact]
        public async Task BD001_ReportsNestedAccessOnSameInstance() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        _accessor.Access(tsn => {
            _accessor.Access(innerTsn => {
                Console.WriteLine(""nested"");
            });
        });
    }
}
";
            var expected = new DiagnosticResult(RdbmsDataAccessorDiagnostics.NestedAccess)
                .WithLocation(10, 13)
                .WithArguments("Access", "Access");

            await CreateTest(source, expected).RunAsync();
        }

        [Fact]
        public async Task BD001_DoesNotReportDifferentInstance() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor1;
    private IRdbmsDataAccessor _accessor2;

    public void TestMethod() {
        _accessor1.Access(tsn => {
            _accessor2.Access(innerTsn => {
                Console.WriteLine(""ok"");
            });
        });
    }
}
";
            await CreateTest(source).RunAsync();
        }

        [Fact]
        public async Task BD001_ReportsNestedAccessAsync() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Threading.Tasks;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public async Task TestMethod() {
        await _accessor.AccessAsync(async tsn => {
            await _accessor.AccessAsync(async innerTsn => {
                Console.WriteLine(""nested"");
            }, default);
        }, default);
    }
}
";
            var expected = new DiagnosticResult(RdbmsDataAccessorDiagnostics.NestedAccess)
                .WithLocation(11, 19)
                .WithArguments("AccessAsync", "AccessAsync");

            await CreateTest(source, expected).RunAsync();
        }
    }
}
