using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.CustomForms.FetchScript {
    public class ForeachBlock : IFetchScriptBlockStatement {
        object input;

        public bool ExecuteBlock(List<string> block) {
            throw new NotImplementedException();
        }

        public bool Init(object[] args) {
            if(args.Length != 1 || !(args[0] is IEnumerable)) {
                Console.WriteLine("foreach requires an enumerable type to iterate on");
                return false;
            }
            input = args[0];
            return true;
        }
    }
}
