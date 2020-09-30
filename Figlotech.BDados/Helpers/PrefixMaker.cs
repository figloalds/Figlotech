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
            string retv = null;
            if (dict.ContainsKey(k)) {
                retv = dict[k];
            } else {
                dict.Add(k, "tb" + new IntEx(seq++).ToString(IntEx.Base26).ToLower());
                revDict[dict[k]] = (parent, child, pkey);
                retv = dict[k];
            }

            //Console.WriteLine($"{k} = {retv}");
            return retv;
        }
        public string GetAliasFor(string parent, string child, string pkey) {
            if (revDict.ContainsKey(parent)) {
                var rdparent = revDict[parent];
                if (revDict.ContainsKey(rdparent.parent)) {
                    var rdparent2 = revDict[rdparent.parent];
                    if (rdparent.parent != "root" && rdparent2.child == child && pkey == rdparent.key) {
                        return rdparent.parent;
                    }
                }
            }
            lock (dict) {
                return GetNewAliasFor(parent, child, pkey);
            }
        }
    }
}
