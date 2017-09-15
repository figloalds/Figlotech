namespace Figlotech.BDados.Authentication {
    public class BasicPermissionContainer : IPermissionsContainer {
        public static int DefaultTransportSize = 128;
        private byte[] _buffer = new byte[DefaultTransportSize];
        public byte[] Buffer {
            get => _buffer;
            set => _buffer = value;
        }
    }
}
