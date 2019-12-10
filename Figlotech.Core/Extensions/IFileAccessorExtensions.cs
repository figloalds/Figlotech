﻿using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Extensions
{
    public static class IFileAccessorExtensions {
        public static async Task Copy(this IFileSystem me, string origin, string destination) {
            await Copy(me, origin, me, destination);
        }
        public static async Task Copy(this IFileSystem me, string origin, IFileSystem remote, string destination) {
            if(me is FileAccessor && remote is FileAccessor) {
                using (var inStream = me.Open(origin, System.IO.FileMode.Open, System.IO.FileAccess.Read)) {
                    using (var outStream = remote.Open(destination, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite)) {
                        inStream.EconomicCopyTo(outStream);
                    }
                }
                return;
            }

            await me.Read(origin, async inStream => {
                await Task.Yield();
                await remote.Write(destination, async outStream => {
                    await Task.Yield();
                    inStream.CopyTo(outStream);
                });
            });
        }
    }
}
