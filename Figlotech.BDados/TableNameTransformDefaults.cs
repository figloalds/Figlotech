using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.TableNameTransformDefaults
{
    public static class TNTDExtension
    {
        public static string ToTableName(this String self) {
            return self;
        }
    }
}
