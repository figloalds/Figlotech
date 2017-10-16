using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public class InstanceAuthorizer
    {
        List<string> auths = new List<string>();
        public String GenerateAuthorization() {
            var value = new RID().AsBase64;
            auths.Add(value);
            return value;
        }

        public bool CheckAuthorization(String auth) {
            return auths.RemoveAll(a=> a == auth) > 0;
        }
    }
}
