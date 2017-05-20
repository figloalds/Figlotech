using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.I18n
{
    public interface IBDadosStringsProvider {
        string AUTH_USER_MAX_ATTEMPTS_EXCEEDED { get; }
        string AUTH_USER_NOT_FOUND { get; }
        string AUTH_PASSWORD_INCORRECT { get; }
        string AUTH_USER_BLOCKED { get; }
        string AUTH_PASSWORDS_MUST_MATCH { get; }
        string AUTH_USER_ALREADY_EXISTS { get; }
        string SCOPY_ACCESSORS_CANNOT_BE_SAME { get; }
    }
}
