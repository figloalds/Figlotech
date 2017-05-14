namespace Figlotech.Autokryptex {
    public interface IEncryptionMethod {
        byte[] Encrypt(byte[] en);

        byte[] Decrypt(byte[] en);
    }
}