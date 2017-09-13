using Microsoft.VisualStudio.TestTools.UnitTesting;
using Figlotech.BDados.FileAcessAbstractions;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.MySqlDataAccessor;

namespace Figlotech.BDados.Tests {
    [TestClass]
    public class DependencyResolverTest
    {
        [TestMethod]
        public void DependencySolverShouldWork()
        {
            DependencyResolver resolver = new DependencyResolver();
            resolver.AddInstance<IFileAccessor>(new FileAccessor("C:\\teste"));
            resolver.AddFactory<SmartCopyOptions>(() => new SmartCopyOptions());
            resolver.AddAbstract<ILogger, Logger>();
            resolver.AddAbstract<SmartCopy, SmartCopy>();
            var sc = new RdbmsDataAccessor<MySqlPlugin>(new DataAccessorConfiguration());
            resolver.SmartResolve(sc);
            Assert.IsNotNull(sc.Logger);
        }
    }
}
