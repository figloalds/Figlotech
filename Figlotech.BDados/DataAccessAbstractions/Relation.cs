using System;
using System.Collections.Generic;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class Relation
    {
        public int ChildIndex;
        public int ParentIndex;
        public String ParentKey;
        public String ChildKey;
        public AggregateBuildOptions AggregateBuildOption;
        public String NewName;
        public List<String> Fields = new List<String>();
    }
}
