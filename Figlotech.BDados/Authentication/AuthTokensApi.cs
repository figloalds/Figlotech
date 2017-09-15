using Figlotech.Autokryptex;
using Figlotech.BDados;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public class AuthToken {
        public DateTime when;
        public String Token;
        public AuthToken(String s, DateTime d) {
            Token = s;
            when = d;
        }
    }

    public class AuthTokensApi {
        public static List<AuthToken> AuthTokens = new List<AuthToken>();
        public static bool ValToken(String Token) {
            var v = (from a in AuthTokens where a.Token == Token select a);
            if (v.Any()) {
                if (DateTime.Now.Subtract(v.First().when).TotalMinutes > 20) {
                    return false;
                }
                else {
                    v.First().when = DateTime.Now;
                    return true;
                }
            }
            return false;
        }

        public static void KillToken(String Token) {
            var v = (from a in AuthTokens where a.Token == Token select a);
            if (v.Any()) {
                AuthTokens.Remove(v.First());
            }
        }

        public static String NewToken() {
            String Auth = Fi.Tech.GenerateIdString("UserCreation", 128);
            AuthTokens.Add(new AuthToken(Auth, DateTime.Now));
            return Auth;
        }
    }
}
