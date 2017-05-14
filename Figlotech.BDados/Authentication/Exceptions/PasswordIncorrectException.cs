using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication.Exceptions {
    public class PasswordIncorrectException : Exception{
        public PasswordIncorrectException(String msg) : base(msg) {

        }
    }
}
