using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Figlotech.Core.FileAcessAbstractions;
using Xunit;

namespace Figlotech.Core.Tests {
    public sealed class FileAccessorTests {
        [Fact]
        public void GetDirectoriesIn_ReturnsFullRootRelativePathsWithForwardSlashes() {
            using var fixture = new TemporaryDirectory();
            Directory.CreateDirectory(Path.Combine(fixture.Path, "registro_mestre_vendas", "2026-07-alpha"));
            Directory.CreateDirectory(Path.Combine(fixture.Path, "registro_mestre_vendas", "2026-08-beta"));
            var accessor = new FileAccessor(fixture.Path + Path.DirectorySeparatorChar);

            var directories = accessor.GetDirectoriesIn("registro_mestre_vendas").OrderBy(x => x).ToArray();

            Assert.Equal(new[] {
                "registro_mestre_vendas/2026-07-alpha",
                "registro_mestre_vendas/2026-08-beta"
            }, directories);
            Assert.All(directories, path => Assert.DoesNotContain('\\', path));
        }

        [Fact]
        public void GetFilesIn_AcceptsEitherInputSeparatorAndReturnsFullRootRelativePaths() {
            using var fixture = new TemporaryDirectory();
            var directory = Path.Combine(fixture.Path, "registro_mestre_vendas", "2026-07");
            Directory.CreateDirectory(directory);
            File.WriteAllText(Path.Combine(directory, "vendas.txt"), "content");
            var accessor = new FileAccessor(fixture.Path);

            var forwardSlashResult = accessor.GetFilesIn("registro_mestre_vendas/2026-07").Single();
            var backslashResult = accessor.GetFilesIn("registro_mestre_vendas\\2026-07").Single();

            Assert.Equal("registro_mestre_vendas/2026-07/vendas.txt", forwardSlashResult);
            Assert.Equal(forwardSlashResult, backslashResult);
        }

        [Fact]
        public async Task MkDirsAsync_CreatesTheEntireChainUnderTheAccessorRoot() {
            using var fixture = new TemporaryDirectory();
            var topDirectoryName = $"mk-{Guid.NewGuid():N}";
            var relative = $"{topDirectoryName}\\year/month/day";
            var expectedPath = Path.Combine(fixture.Path, topDirectoryName, "year", "month", "day");
            var accidentalWorkingDirectoryPath = Path.GetFullPath(topDirectoryName);
            Directory.CreateDirectory(Path.Combine(fixture.Path, topDirectoryName, "year"));
            var accessor = new FileAccessor(fixture.Path);

            try {
                await accessor.MkDirsAsync(relative);

                Assert.True(Directory.Exists(expectedPath));
                Assert.False(Directory.Exists(accidentalWorkingDirectoryPath));
            } finally {
                if (Directory.Exists(accidentalWorkingDirectoryPath)) {
                    Directory.Delete(accidentalWorkingDirectoryPath, true);
                }
            }
        }

        [Fact]
        public void WriteAllText_AcceptsMixedSeparatorsAndKeepsThePathInsideTheRoot() {
            using var fixture = new TemporaryDirectory();
            var accessor = new FileAccessor(fixture.Path);

            accessor.WriteAllText("one\\two/three.txt", "content");

            Assert.Equal("content", File.ReadAllText(Path.Combine(fixture.Path, "one", "two", "three.txt")));
        }

        [Fact]
        public void WriteAllText_RejectsTraversalIntoSiblingWhoseNameSharesTheRootPrefix() {
            using var fixture = new TemporaryDirectory();
            var sibling = fixture.Path + "-outside";
            var relative = $"../{Path.GetFileName(sibling)}/probe.txt";
            var accessor = new FileAccessor(fixture.Path);

            try {
                Assert.ThrowsAny<Exception>(() => accessor.WriteAllText(relative, "must not escape"));
                Assert.False(File.Exists(Path.Combine(sibling, "probe.txt")));
            } finally {
                if (Directory.Exists(sibling)) {
                    Directory.Delete(sibling, true);
                }
            }
        }

        private sealed class TemporaryDirectory : IDisposable {
            public string Path { get; } = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "Figlotech.Core.Tests",
                Guid.NewGuid().ToString("N"));

            public TemporaryDirectory() {
                Directory.CreateDirectory(Path);
            }

            public void Dispose() {
                if (Directory.Exists(Path)) {
                    Directory.Delete(Path, true);
                }
            }
        }
    }
}
