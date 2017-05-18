using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication
{
    public static class IBDadosPermissionExtensions {
        public static bool CanCreate(this IBDadosPermission input) {
            return (input.Permission & (int)BDadosPermissions.Create) > 0;
        }
        public static bool CanRead(this IBDadosPermission input) {
            return (input.Permission & (int)BDadosPermissions.Read) > 0;
        }
        public static bool CanUpdate (this IBDadosPermission input) {
            return (input.Permission & (int)BDadosPermissions.Update) > 0;
        }
        public static bool CanDelete (this IBDadosPermission input) {
            return (input.Permission & (int)BDadosPermissions.Delete) > 0;
        }
        public static bool CanAuthorize (this IBDadosPermission input) {
            return (input.Permission & (int)BDadosPermissions.Authorize) > 0;
        }

        public static void SetPermission(this IBDadosPermission input, BDadosPermissions permission, bool value) {
            if (value)
                input.Permission |= (int) permission;
            else
                input.Permission ^= (input.Permission ^ (int) permission);
        }
    }
}
