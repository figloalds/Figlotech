using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Attributes {
    public enum DataStringComparisonType {
        ByLikeliness,
        ByExactValue
    }

    public class ComparisonTypeAttribute : Attribute {
        public DataStringComparisonType Type;
        public ComparisonTypeAttribute(DataStringComparisonType type) {
            Type = type;
        }
    }
}
