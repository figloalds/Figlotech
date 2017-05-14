using System;
using System.Runtime.Serialization;

namespace Figlotech.BDados
{
    [Serializable]
    internal class BDadosThrowableReturn : Exception
    {
        public object o;
        
        public BDadosThrowableReturn(object o) {
            this.o = o;
        }
    }
}