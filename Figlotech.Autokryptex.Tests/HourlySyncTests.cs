using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using Figlotech.Autokryptex.EncryptMethods;

namespace Figlotech.Autokryptex.Tests
{
    [TestClass]
    public class HourlySyncTests {
        [TestMethod]
        public void HourlySyncShouldValidate() {
            var hourly = HourlySyncCode.Generate("This is just a test");
            Assert.IsTrue(HourlySyncCode.Validate(hourly, "This is just a test"));
        }
    }
}
