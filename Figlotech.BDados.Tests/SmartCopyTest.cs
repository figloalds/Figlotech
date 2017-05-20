using Microsoft.VisualStudio.TestTools.UnitTesting;
using Figlotech.BDados.FileAcessAbstractions;

namespace Figlotech.BDados.Tests {
    [TestClass]
    public class SmartCopyTest
    {
        [TestMethod]
        public void ItShouldWork()
        {
            FileAccessor fa1 = new FileAccessor("C:\\teste");
            FileAccessor fa2 = new FileAccessor("C:\\teste2");

            var copy = new SmartCopy(fa1, new SmartCopyOptions());
            copy.SetRemote(fa2);

            copy.mirrorUp("");
        }
    }
}
