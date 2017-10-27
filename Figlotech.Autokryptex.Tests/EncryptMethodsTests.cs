using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using Figlotech.Core.Autokryptex.EncryptMethods;
using Figlotech.BDados;
using Figlotech.Core;

namespace Figlotech.Core.Autokryptex.Tests
{
    [TestClass]
    public class EncryptMethodsTests {

        private bool TestIEncryptionMethod(IEncryptionMethod c1, String input) {

            CrossRandom.UseAppSecret("Cryptographic password");

            byte[] bytes = Encoding.UTF8.GetBytes(input);

            var encryptedBytes = c1.Encrypt(bytes);

            Fi.Tech.WriteLine(Encoding.UTF8.GetString(encryptedBytes));

            var decryptedBytes = c1.Decrypt(encryptedBytes);

            String output = Encoding.UTF8.GetString(decryptedBytes);
            Assert.AreEqual(input, output);
            return input == output;
        }

        [TestMethod]
        public void CrazyEncryptorShouldWork() {
            String input = "Encryption Test";
            AutokryptexEncryptor c1 = new AutokryptexEncryptor("Some good key");
            Assert.IsTrue(TestIEncryptionMethod(c1, input));
        }
        [TestMethod]
        public void EnigmaEnkryptorShouldBeReversible() {
            String input = "Encryption Test";
            EnigmaEncryptor c1 = new EnigmaEncryptor(456);
            Assert.IsTrue(TestIEncryptionMethod(c1, input));
        }
        [TestMethod]
        public void ByteBlurShouldBeReversible() {
            String input = "Encryption Test";
            BinaryBlur c1 = new BinaryBlur();
            Assert.IsTrue(TestIEncryptionMethod(c1, input));
        }
        [TestMethod]
        public void ByteNegativatorShouldBeReversible() {
            String input = "Encryption Test";
            BinaryNegativation c1 = new BinaryNegativation();
            Assert.IsTrue(TestIEncryptionMethod(c1, input));
        }
        [TestMethod]
        public void ByteScramblerShouldBeReversible() {
            String input = "Encryption Test";
            BinaryScramble c1 = new BinaryScramble(456);
            Assert.IsTrue(TestIEncryptionMethod(c1, input));
        }
        [TestMethod]
        public void AesEncryptorShouldWork() {
            String input = "Encryption Test";
            AesEncryptor c1 = new AesEncryptor("HELLO WORLD", 456);
            Assert.IsTrue(TestIEncryptionMethod(c1, input));
        }
    }
}
