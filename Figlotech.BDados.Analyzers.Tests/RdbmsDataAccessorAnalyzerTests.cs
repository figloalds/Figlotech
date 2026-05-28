using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Figlotech.BDados.Analyzers.Tests {
    [TestClass]
    public class RdbmsDataAccessorAnalyzerTests {
        private static async Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync(string source) {
            var syntaxTree = CSharpSyntaxTree.ParseText(source);
            var references = new List<MetadataReference> {
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Linq.Enumerable).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Threading.Tasks.Task).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Figlotech.BDados.DataAccessAbstractions.IRdbmsDataAccessor).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Figlotech.Core.Interfaces.IDataObject).Assembly.Location)
            };
            
            // Add essential runtime references
            var runtimeDir = Path.GetDirectoryName(typeof(object).Assembly.Location);
            var essentialRefs = new[] {
                "netstandard.dll",
                "mscorlib.dll",
                "System.Runtime.dll",
                "System.Private.CoreLib.dll",
                "System.Data.Common.dll",
                "System.Threading.dll",
                "System.Threading.Tasks.dll",
                "System.Linq.Expressions.dll",
                "System.Collections.dll",
                "System.Console.dll"
            };
            foreach (var refName in essentialRefs) {
                var refPath = Path.Combine(runtimeDir, refName);
                if (File.Exists(refPath)) {
                    references.Add(MetadataReference.CreateFromFile(refPath));
                }
            }

            var compilation = CSharpCompilation.Create(
                "TestCompilation",
                new[] { syntaxTree },
                references,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

            // Check for compilation errors
            var compileDiagnostics = compilation.GetDiagnostics();
            var compileErrors = compileDiagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
            if (compileErrors.Any()) {
                throw new InvalidOperationException($"Compilation errors: {string.Join("\n", compileErrors.Select(e => e.GetMessage()))}");
            }

            var analyzer = new RdbmsDataAccessorAnalyzer();
            var compilationWithAnalyzers = compilation.WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(analyzer));
            return await compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync();
        }

        [TestMethod]
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
            var diagnostics = await GetDiagnosticsAsync(source);
            var bd001Diagnostics = diagnostics.Where(d => d.Id == "BD001").ToList();
            Assert.AreEqual(1, bd001Diagnostics.Count);
            StringAssert.Contains(bd001Diagnostics[0].GetMessage(), "Access");
        }

        [TestMethod]
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
            var diagnostics = await GetDiagnosticsAsync(source);
            var bd001Diagnostics = diagnostics.Where(d => d.Id == "BD001").ToList();
            Assert.AreEqual(0, bd001Diagnostics.Count);
        }

        [TestMethod]
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
            var diagnostics = await GetDiagnosticsAsync(source);
            var bd001Diagnostics = diagnostics.Where(d => d.Id == "BD001").ToList();
            Assert.AreEqual(1, bd001Diagnostics.Count);
            StringAssert.Contains(bd001Diagnostics[0].GetMessage(), "AccessAsync");
        }

        [TestMethod]
        public async Task BD002_ReportsMissingTransaction() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        _accessor.Access(tsn => {
            var result = _accessor.Query<TestType>();
        });
    }
}

public class TestType {
    public int Id { get; set; }
}
";
            var diagnostics = await GetDiagnosticsAsync(source);
            var bd002Diagnostics = diagnostics.Where(d => d.Id == "BD002").ToList();
            Assert.AreEqual(1, bd002Diagnostics.Count);
            StringAssert.Contains(bd002Diagnostics[0].GetMessage(), "Query");
        }

        [TestMethod]
        public async Task BD002_DoesNotReportWithTransaction() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        _accessor.Access(tsn => {
            var result = _accessor.Query<TestType>(tsn);
        });
    }
}

public class TestType {
    public int Id { get; set; }
}
";
            var diagnostics = await GetDiagnosticsAsync(source);
            var bd002Diagnostics = diagnostics.Where(d => d.Id == "BD002").ToList();
            Assert.AreEqual(0, bd002Diagnostics.Count);
        }

        [TestMethod]
        public async Task BD002_DoesNotReportOutsideAccess() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        var result = _accessor.Query<TestType>();
    }
}

public class TestType {
    public int Id { get; set; }
}
";
            var diagnostics = await GetDiagnosticsAsync(source);
            var bd002Diagnostics = diagnostics.Where(d => d.Id == "BD002").ToList();
            Assert.AreEqual(0, bd002Diagnostics.Count);
        }

        [TestMethod]
        public async Task BD002_ReportsExecuteMissingTransaction() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        _accessor.Access(tsn => {
            _accessor.Execute(null);
        });
    }
}
";
            var diagnostics = await GetDiagnosticsAsync(source);
            var bd002Diagnostics = diagnostics.Where(d => d.Id == "BD002").ToList();
            Assert.AreEqual(1, bd002Diagnostics.Count);
            StringAssert.Contains(bd002Diagnostics[0].GetMessage(), "Execute");
        }
    }
}
