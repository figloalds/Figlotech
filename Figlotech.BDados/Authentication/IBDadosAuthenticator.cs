using Figlotech.BDados.Authentication;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    /// This is a trick I've learned from MICROSOFT (reading their H4CKZ0R sources)
    /// You can create an abstract non-generic class so that you can reference
    /// Its abstract non-genericness, but still use generiqued dudes to do the
    /// real work. Thanks buds.
    /// But in this case I'm creating an interface to open more expansion possibilites
    /// with the least possible ammount of suffering
    public interface IBDadosAuthenticator {
        /// <summary>
        /// This is supposed to return an authentication token.
        /// </summary>
        /// <param name="userName">Self explanatory: Username</param>
        /// <param name="password">Self explanatory: Password</param>
        /// <returns></returns>
        IBDadosUserSession Login(String userName, String password);
        /// <summary>
        /// This is supposed to log the user off, destroy the token do whatever
        /// but not allow that token to access again.
        /// </summary>
        /// <param name="token">Self explanatory: Token</param>
        /// <returns></returns>
        void Logoff(IBDadosUserSession session);
        /// <summary>
        /// This is supposed to return the user session or NULL in case of
        /// no session found.
        /// </summary>
        /// <param name="token">Self explanatory: Token</param>
        /// <returns></returns>
        IBDadosUserSession GetSession(String token);


        IBDadosUser ForceCreateUser(string userName, string password);

        IBDadosUser CreateUserSecure(string userName, string password, string confirmPassword);

        bool ForcePassword(IBDadosUser user, string newPassword);

        bool ChangeUserPasswordSecure(IBDadosUser user, string oldPassword, string newPassword, string newPasswordConfirmation);
    }
}
