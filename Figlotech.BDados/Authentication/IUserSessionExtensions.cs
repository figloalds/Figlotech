using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication
{
    public static class IUserSessionExtensions {
                
        public static bool CanCreate(this IUserSession input, int permission) {
            return FTH.CheckForPermission(input?.Permission?.Buffer, Acl.Create, permission);
        }
        public static bool CanRead (this IUserSession input, int permission) {
            return FTH.CheckForPermission(input?.Permission?.Buffer, Acl.Read, permission);
        }
        public static bool CanUpdate (this IUserSession input, int permission) {
            return FTH.CheckForPermission(input?.Permission?.Buffer, Acl.Update, permission);
        }
        public static bool CanDelete (this IUserSession input, int permission) {
            return FTH.CheckForPermission(input?.Permission?.Buffer, Acl.Delete, permission);
        }
        public static bool CanAuthorize (this IUserSession input, int permission) {
            return FTH.CheckForPermission(input?.Permission?.Buffer, Acl.Authorize, permission);
        }
    }
}
