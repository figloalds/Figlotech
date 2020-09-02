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

        Dictionary<string, (string parent, string child, string key)> revDict = new Dictionary<string, (string parent, string child, string key)>();

        public string GetNewAliasFor(string parent, string child, string pkey) {
            var k = $"{parent}_{child}_{pkey}".ToLower();
            if (dict.ContainsKey(k)) {
                return dict[k];
            } else {
                dict.Add(k, "tb" + new IntEx(seq++).ToString(IntEx.Base26).ToLower());
                revDict[dict[k]] = (parent, child, pkey);
                return dict[k];
            }
        }
        public string GetAliasFor(string parent, string child, string pkey) {
            if (parent != "tba" && revDict.ContainsKey(parent)) {
                var rdparent = revDict[parent];
                var rdparent2 = revDict[rdparent.parent];
                if (rdparent.parent != "root" && rdparent2.child == child) {
                    return rdparent.parent;
                }
            }
            lock (dict) {
                return GetNewAliasFor(parent, child, pkey);
            }
        }
    }
}
