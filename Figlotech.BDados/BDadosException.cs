using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.BDados
{
    public class BDadosException : Exception {
        public List<IDataObject> AffectedObjects { get; private set; }

        public BDadosException(String message) : base(message) {

        }
        public BDadosException(String message, Exception inner) : base(message, inner) {

        }
        public BDadosException(String message, List<IDataObject> objects, Exception inner) : base(message, inner) {
            AffectedObjects = objects;
        }
    }
}
