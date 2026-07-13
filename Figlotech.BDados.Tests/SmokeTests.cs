using Figlotech.BDados.DataAccessAbstractions;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class SmokeTests {
        [Fact]
        public void TestProjectLoadsBDadosAssembly() {
            Assert.NotNull(typeof(RdbmsDataAccessor).Assembly);
        }
    }
}
