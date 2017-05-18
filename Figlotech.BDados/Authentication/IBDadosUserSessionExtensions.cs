using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication
{
    public static class IBDadosUserSessionExtensions {
        public static bool CanRead (this IBDadosUserSession input, String Module, String Resource) {
            return input.Permissions.Where(
                p => p.Module == Module && p.Resource == Resource
            ).FirstOrDefault()?.CanRead() ?? false;
        }
        public static bool CanUpdate (this IBDadosUserSession input, String Module, String Resource) {
            return input.Permissions.Where(
                p => p.Module == Module && p.Resource == Resource
            ).FirstOrDefault()?.CanUpdate() ?? false;
        }
        public static bool CanCreate (this IBDadosUserSession input, String Module, String Resource) {
            return input.Permissions.Where(
                p => p.Module == Module && p.Resource == Resource
            ).FirstOrDefault()?.CanCreate() ?? false;
        }
        public static bool CanDelete (this IBDadosUserSession input, String Module, String Resource) {
            return input.Permissions.Where(
                p => p.Module == Module && p.Resource == Resource
            ).FirstOrDefault()?.CanDelete() ?? false;
        }
        public static bool CanAuthorize (this IBDadosUserSession input, String Module, String Resource) {
            return input.Permissions.Where(
                p => p.Module == Module && p.Resource == Resource
            ).FirstOrDefault()?.CanAuthorize() ?? false;
        }
    }
}
