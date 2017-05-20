using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Interfaces;

namespace Figlotech.BDados.Authentication
{
    public class BasicPermissionContainer : IBDadosPermissionsContainer {
        public static int DefaultTransportSize = 128;
        private byte[] _buffer = new byte[DefaultTransportSize];
        public byte[] Buffer {
            get => _buffer;
            set => _buffer = value;
        }
    }
}
