using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Extensions
{
    public static class IFileAccessorExtensions {
        public static void Copy(this IFileAccessor me, string origin, string destination) {
            Copy(me, origin, me, destination);
        }
        public static void Copy(this IFileAccessor me, string origin, IFileAccessor remote, string destination) {
            if(me is FileAccessor && remote is FileAccessor) {
                using (var inStream = me.Open(origin, System.IO.FileMode.Open, System.IO.FileAccess.Read)) {
                    using (var outStream = remote.Open(destination, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite)) {
                        inStream.EconomicCopyTo(outStream);
                    }
                }
                return;
            }

            me.Read(origin, inStream => {
                remote.Write(destination, outStream => {
                    inStream.CopyTo(outStream);
                });
            });
        }
    }
}
