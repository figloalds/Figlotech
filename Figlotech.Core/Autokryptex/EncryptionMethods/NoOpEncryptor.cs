namespace Figlotech.Core.Autokryptex.EncryptionMethods {
    public sealed class NoOpEncryptor : IEncryptionMethod {
        public byte[] Decrypt(byte[] en) {
            return en;
        }

        public byte[] Encrypt(byte[] en) {
            return en;
        }
    }
}
