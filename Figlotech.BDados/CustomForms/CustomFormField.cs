using System;
using System.Collections.Generic;

namespace Figlotech.BDados.CustomForms {

    public sealed class PossibleOption {
        public int Id;
        public String Description;
    }

    public sealed class UiRole {
        public bool CanFilterBy;
        public bool AppearsOnTables;
    }

    public sealed class CustomFormField {
        public String Name;
        public String Description;
        public String Type;
        public int Size;
        public List<PossibleOption> PossibleOptions;
        public UiRole Role;
    }

}