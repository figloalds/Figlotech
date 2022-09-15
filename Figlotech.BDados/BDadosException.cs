using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Figlotech.BDados
{
    public sealed class BDadosException : Exception {
        public List<IDataObject> AffectedObjects { get; private set; }
        public List<string[]> StackData { get; private set; }

        public BDadosException(String message) : base(message) {

        }
        public BDadosException(String message, Exception inner) : base(message, inner) {

        }
        public BDadosException(String message, List<string[]> StackData, List<IDataObject> objects, Exception inner) : base(message, inner) {
            AffectedObjects = objects;
            this.StackData = StackData;
        }
    }
}
