using System;
using System.Collections.Generic;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class Relation
    {
        internal int ChildIndex;
        internal int ParentIndex;
        internal String ParentKey;
        internal String ChildKey;
        internal AggregateBuildOptions AssemblyOption;
        internal String NewName;
        internal List<String> Fields = new List<String>();
    }
}
