using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication
{
    public static class IBDadosPermissionExtensions {

        public static bool CanCreate(this IBDadosPermission input, int permission) {
            return FTH.CheckForPermission(input.Buffer, Acl.Create, permission);
        }
        public static bool CanRead(this IBDadosPermission input, int permission) {
            return FTH.CheckForPermission(input.Buffer, Acl.Read, permission);
        }
        public static bool CanUpdate (this IBDadosPermission input, int permission) {
            return FTH.CheckForPermission(input.Buffer, Acl.Update, permission);
        }
        public static bool CanDelete (this IBDadosPermission input, int permission) {
            return FTH.CheckForPermission(input.Buffer, Acl.Delete, permission);
        }
        public static bool CanAuthorize (this IBDadosPermission input, int permission) {
            return FTH.CheckForPermission(input.Buffer, Acl.Authorize, permission);
        }

        public static byte[] Overlap(this IBDadosPermission input, IBDadosPermission other) {
            var largerBuffer = Math.Max(input.Buffer.Length, other.Buffer.Length);
            var smallerBuffer = Math.Min(input.Buffer.Length, other.Buffer.Length);
            var buffer = new byte[largerBuffer];
            for(int i = 0; i < largerBuffer; i++) {
                if (i < input.Buffer.Length)
                    buffer[i] |= input.Buffer[i];
                if (i < other.Buffer.Length)
                    buffer[i] |= other.Buffer[i];
            }

            return buffer;
        }
    }
}
