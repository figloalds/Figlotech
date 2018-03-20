using System;
using System.Collections.Generic;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class JoiningTable
    {
        public JoiningTable() { }
        public Type ValueObject = null;
        public String TableName = null;
        public JoinType Type = JoinType.LEFT;
        public String Args = null;
        public String Prefix = null;
        public String Alias = null;
        public List<String> Columns = new List<String>();
    }
}
