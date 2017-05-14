
/**
 * Figlotech::Database::Entity::FieldAttribute
 * This is used by reflection by RepositoryValueObject and BDados 
 * for figuring out how the inherited RepositoryValueObject should treat a given field
 * This is also heavily used by the BDados.CheckStructure method to figure out how
 * any given field is represented in the database as a Column.
 * 
 *@Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Attributes {
    public enum DisplayFormatType {
        Text,
        Date,
        Decimal,
        Integer,
        Money,
        NoFormat,
    }
    public class DisplayAttribute : Attribute
    {
        public String Name;
        public int Order;
        public DisplayFormatType Format;
        public String FormatString;
        
        public DisplayAttribute(String name, int order, DisplayFormatType format = DisplayFormatType.NoFormat, String formatString = null) {
            Name = name;
            Order = order;
            Format = format;
            FormatString = formatString;
        }
    }
}
