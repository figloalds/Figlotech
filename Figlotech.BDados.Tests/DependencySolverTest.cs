using Microsoft.VisualStudio.TestTools.UnitTesting;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Interfaces;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.Core;

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
