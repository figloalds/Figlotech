using Microsoft.VisualStudio.TestTools.UnitTesting;
using Figlotech.Core.FileAcessAbstractions;

namespace Figlotech.BDados.Tests {
    [TestClass]
    public class SmartCopyTest
    {
        [TestMethod]
        public void ItShouldWork()
        {
            FileAccessor fa1 = new FileAccessor("C:\\smartCopyOut");
            FileAccessor fa2 = new FileAccessor("C:\\smartCopyOut");
            //var fa2 = new BlobFileAccessor("felyper", "VaKDbICKepMsYKYQ6i01B12AeBPmo4MCq7plnDlVrlLiFGAzwaJ7PAFNDmJUKzmktKdykT/bRVW+5x7PfDFKAQ==", "erpslv2");

            var copy = new SmartCopy(fa1, new SmartCopyOptions() {
                UseGZip = true,
                UseHash = true,
                UseHashList = true,
                Multithreaded = true,
                Recursive = true
            });

            copy.SetRemote(fa2);
            copy.Excludes.Add("cef_redist.zip");
            copy.MirrorDown("");
        }
    }
}
