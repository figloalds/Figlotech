using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Extensions
{
    public static class IEnumerableBDadosExtensions {
        public static RecordSet<T> ToRecordSet<T>(this IEnumerable<T> me) where T : IDataObject, new() {
            var rs = new RecordSet<T>();
            rs.AddRange(me);
            return rs;
        }
    }
}
