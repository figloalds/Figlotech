using Figlotech.BDados;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers {
    public class PrefixMaker {

        int seq = 0;
        Dictionary<string, string> dict = new Dictionary<string, string>();

        public string GetAliasFor(string parent, string child, string pkey) {
            var k = $"{parent}_{child}_{pkey}".ToLower();
            lock(dict) {
                if (dict.ContainsKey(k)) {
                    return dict[k];
                } else {
                    dict.Add(k, "tb"+new IntEx(seq++).ToString(IntEx.Base26).ToLower());
                    return dict[k];
                }
            }
        }
    }
}
