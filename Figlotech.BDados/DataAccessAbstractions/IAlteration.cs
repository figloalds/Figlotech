using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IAlteration : IDataObject {
        String Origin { get; set; }
        String Field { get; set; }
        String FieldType { get; set; }
        String OldValue { get; set; }
        String NewValue { get; set; }
    }
}
