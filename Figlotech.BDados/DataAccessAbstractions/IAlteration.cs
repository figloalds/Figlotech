using Figlotech.Core.Interfaces;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IAlteration : ILegacyDataObject {
        String Origin { get; set; }
        String Field { get; set; }
        String FieldType { get; set; }
        String OldValue { get; set; }
        String NewValue { get; set; }
    }
}
