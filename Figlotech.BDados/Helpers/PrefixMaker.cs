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
        public string GetAliasFor(string parent, string child) {
            if(dict.ContainsKey($"{parent}_{child}")) {
                return dict[$"{parent}_{child}"];
            } else {
                dict.Add($"{parent}_{child}", "tb"+new IntEx(seq++).ToString(IntEx.Base26).ToLower());
                return dict[$"{parent}_{child}"];
            }
        }
    }
}
