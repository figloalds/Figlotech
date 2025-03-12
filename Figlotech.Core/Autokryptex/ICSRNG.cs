namespace Figlotech.Core.Autokryptex {
    public interface ICSRNG {
        int Next();
        int Next(int Maximum);
        int Next(int Minimum, int Maximum);
    }
}