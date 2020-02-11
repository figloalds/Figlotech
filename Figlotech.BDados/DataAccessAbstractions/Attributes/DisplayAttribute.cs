
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

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    public enum DisplayFormatType {
        Text,
        Date,
        Decimal,
        Integer,
        Money,
        NoFormat,
    }

    public interface ICustomDisplayFormatter
    {
        object Format(object input);
    }

    public abstract class CustomDisplayFormatter<T>
    {
        public object Format(object input) {
            return Format((T) input);
        }
        public abstract object Format(T input);
    }

    /// <summary>
    /// Tells tools how to display this field on a screen.
    /// can be used to generate forms or tables from dataobject metadata
    /// </summary>
    public class DisplayAttribute : Attribute
    {
        public String Name;
        public bool CanFilterBy;
        public int Order;
        public DisplayFormatType Format;
        public String FormatString;
        public Type CustomFormatter;
    }
}
