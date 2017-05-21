using Microsoft.VisualStudio.TestTools.UnitTesting;
using Figlotech.BDados.FileAcessAbstractions;
using Figlotech.BDados.Helpers;

namespace Figlotech.BDados.Tests {
    [TestClass]
    public class DependencyResolverTest
    {
        [TestMethod]
        public void DependencySolverShouldWork()
        {
            DependencyResolver resolver = new DependencyResolver();
            resolver.AddInstance<IFileAccessor>(new FileAccessor("C:\\teste"));
            resolver.AddFactory<ISmartCopyOptions>(() => new SmartCopyOptions());
            resolver.AddAbstract<SmartCopy, SmartCopy>();
            var copier = resolver.Resolve<SmartCopy>();
            Assert.IsNotNull(copier);
        }
    }
}
