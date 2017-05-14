using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.BDados
{
    public class BDadosException : Exception
    {
        public BDadosException(String message) : base(message) {
            
        }
    }
}
