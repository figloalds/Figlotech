using Figlotech.Core.Autokryptex;
using Xunit;

namespace Figlotech.Core.Tests {
    public class EnigmaEncryptorOverflowTests {
        private static readonly int[] ExpectedCompatibleSequence = {
            121, 192, 204, 241, 199, 216, 110, 81, 74, 250, 213, 31
        };

        [Fact]
        public void LegacyEnigmaEncryptor_PreviouslyOverflowingSeed_RoundTrips() {
            var encryptor = new LegacyEnigmaEncryptor(-99719);
            byte[] plaintext = { 0 };

            byte[] ciphertext = encryptor.Encrypt(plaintext);

            Assert.Equal(plaintext, encryptor.Decrypt(ciphertext));
        }

        [Fact]
        public void EnigmaEncryptor_PreviouslyOverflowingSeed_RoundTrips() {
            var encryptor = new EnigmaEncryptor(-99719);
            byte[] plaintext = { 0 };

            byte[] ciphertext = encryptor.Encrypt(plaintext);

            Assert.Equal(plaintext, encryptor.Decrypt(ciphertext));
        }

        [Fact]
        public void LegacyFiRandom_PreviouslySuccessfulSequence_RemainsCompatible() {
            var random = new LegacyFiRandom(12345);

            Assert.Equal(ExpectedCompatibleSequence, GenerateSequence(random));
        }

        [Fact]
        public void FiRandom_PreviouslySuccessfulSequence_RemainsCompatible() {
            var random = new FiRandom(12345);

            Assert.Equal(ExpectedCompatibleSequence, GenerateSequence(random));
        }

        private static int[] GenerateSequence(ICSRNG random) {
            var values = new int[ExpectedCompatibleSequence.Length];
            for (int i = 0; i < values.Length; i++) {
                values[i] = random.Next(i + 1, 256);
            }
            return values;
        }
    }
}
