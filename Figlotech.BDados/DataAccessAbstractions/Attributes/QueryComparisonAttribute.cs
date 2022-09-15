using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    public enum DataStringComparisonType {
        Containing,
        ExactValue,
        IgnoreCase,
        StartingWith,
        EndingWith
    }
    /// <summary>
    /// Allows IDataAccessors do know how to query for this field on the database
    /// can be useful for example a "username" field is IgnoreCase, but an item description 
    /// could use StartsWith for search purposes, this removes a lot of SQL and C# programming 
    /// effort from the programmer's hands and leaves it more declarative
    /// </summary>
    public sealed class QueryComparisonAttribute : Attribute {
        public DataStringComparisonType Type;
        public QueryComparisonAttribute(DataStringComparisonType type) {
            Type = type;
        }
    }
}
