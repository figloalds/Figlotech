using System;
using System.Collections.Generic;

namespace Figlotech.Core {
    public sealed class InstanceAuthorizer {
        readonly List<string> auths = new List<string>();
        public String GenerateAuthorization() {
            var value = new RID().AsBase64;
            auths.Add(value);
            return value;
        }

        public bool CheckAuthorization(String auth) {
            return auths.RemoveAll(a => a == auth) > 0;
        }
    }
}
