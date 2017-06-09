using Figlotech.BDados.DataAccessAbstractions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Figlotech.BDados.Authentication {

    public class Attempt {
        public String User;
        public bool Lock = false;
        public DateTime Timestamp = DateTime.UtcNow;

        public static int maxAttemptsToLock = 12;
        public static TimeSpan lockTime = TimeSpan.FromMinutes(3);
    }

    public class UserSession {
        public static int MaximumInactiveSpanInMinutes = 60;

        public UserSession(IAuthenticator auth) {
            Authenticator = auth;
        }

        public String UserRID;
        public IUserSession Session;
        public String Username;
        public String Token;
        public DateTime Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));

        public void KeepAlive() {
            Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));
        }

        [JsonIgnore]
        protected IAuthenticator Authenticator;

        public IPermissionsContainer Permissions;

        public IUser User { get; set; }

        public void Logoff() {
            Authenticator.Logoff(Session);
        }
    }

    public class BDadosAuthenticator<TUser, TSession>
        : IAuthenticator
        where TUser: IUser, new()
        where TSession: IUserSession, new() {

        public IDataAccessor DataAccessor { get; set; }

        public BDadosAuthenticator(IDataAccessor dataAccessor) {
            DataAccessor = dataAccessor;
        }

        private List<UserSession> Sessions = new List<UserSession>();
        private List<Attempt> TrackAttempts = new List<Attempt>();
        public long globalId;
        //static List<string> RanChecks = new List<string>();

        public void TrackAttempt(String user, bool Success) {
            TrackAttempts.RemoveAll(a =>
                    DateTime.UtcNow.Subtract(a.Timestamp) > Attempt.lockTime);

            var atts = TrackAttempts.Where(
                (a) =>
                    a.User == user
            );
            if (Success) {
                TrackAttempts.RemoveAll(a=> a.User == user);
            }
            if (atts.Any()) {
                if (atts.Count() >= Attempt.maxAttemptsToLock) {
                    throw new ValidationException(String.Format(FTH.Strings.AUTH_USER_MAX_ATTEMPTS_EXCEEDED, Attempt.maxAttemptsToLock));
                }
            }
        }

        private String New(IUser User, TSession Session, String Token) {
            UserSession s = new UserSession(this);
            s.Token = Token;
            s.UserRID = User.RID;
            s.Session = Session;
            s.Permissions = User.GetPermissionsContainer();
            Sessions.Add(s);
            return s.Token;
        }

        public IUser CheckLogin(IUser loadedUser, String password) {
            
            String criptPass = AuthenticationUtils.HashPass(password, loadedUser.RID);
            if(loadedUser.Password != criptPass) {
                TrackAttempt(loadedUser?.RID, false);
                throw new ValidationException(FTH.Strings.AUTH_PASSWORD_INCORRECT);
            }
            if (!loadedUser.isActive) {
                TrackAttempt(loadedUser?.RID, false);
                throw new ValidationException(FTH.Strings.AUTH_USER_BLOCKED);
            }
            return loadedUser;
        }

        public IUserSession Login(IUser user, string password)
        {
            if (user == null) {
                //TrackAttempt(user?.RID, false);
                throw new ValidationException(FTH.Strings.AUTH_USER_NOT_FOUND);
            }
            user = CheckLogin(user, password);
            if (user != null) {
                var sess = new TSession {
                    User = user.RID,
                    isActive = true,
                    Token = FTH.GenerateIdString($"Login:{user.Username};"),
                    StartTime = DateTime.UtcNow,
                    EndTime = null,
                    Permission = user.GetPermissionsContainer()
                };
                if (DataAccessor.SaveItem(sess)) {
                    return sess;
                }
            }
            return default(TSession);
        }

        public bool Exists(String Token) {
            return GetSession(Token) != null;
        }

        public void Logoff(IUserSession s) {
            Sessions.RemoveAll(a=> a.Token == s.Token);
            if(DataAccessor is IRdbmsDataAccessor) {
                (DataAccessor as IRdbmsDataAccessor).Access((bd) => {
                    // DID ANYONE SAY DRAGONS?!
                    (bd).Execute($"UPDATE {typeof(TSession).Name.ToLower()} SET Active=0 WHERE Token=@1", s);
                });
            } else {
                var session = DataAccessor.LoadByRid<TSession>(s.RID);
                session.isActive = false;
                DataAccessor.SaveItem(session);
            }
        }

        public IUserSession GetSession(String Token) {
            var v = (from a in Sessions where a.Token == Token select a);
            if (v.Any())
                return v.First().Session;
            else {
                var fetchSession = DataAccessor.LoadAll<TSession>((us) => us.Token.Equals(Token));
                if (fetchSession.Any()) {
                    var User = DataAccessor.LoadByRid<TUser>(fetchSession.First().User);
                    UserSession sess = new UserSession(this);
                    sess.Token = Token;
                    sess.UserRID = User.RID;
                    sess.Session = fetchSession.First();
                    sess.Permissions = User.GetPermissionsContainer();
                    var retv = fetchSession.FirstOrDefault();
                    retv.Permission = User.GetPermissionsContainer();
                    Sessions.Add(sess);
                    return fetchSession.FirstOrDefault();
                }
            }
            return null;
        }

        public bool CanCreate(IUserSession Session, int permission) {
            return Session?.CanCreate(permission) ?? false;
        }
        public bool CanRead(IUserSession Session, int permission) {
            return Session?.CanRead(permission) ?? false;
        }
        public bool CanUpdate(IUserSession Session, int permission) {
            return Session?.CanUpdate(permission) ?? false;
        }
        public bool CanDelete(IUserSession Session, int permission) {
            return Session?.CanDelete(permission) ?? false;
        }
        public bool CanAuthorize(IUserSession Session, int permission) {
            return Session?.CanAuthorize(permission) ?? false;
        }
        public bool CanCreate(String Token, int permission) {
            return CanCreate(GetSession(Token), permission);
        }
        public bool CanRead(String Token, int permission) {
            return CanRead(GetSession(Token), permission);
        }
        public bool CanUpdate(String Token, int permission) {
            return CanUpdate(GetSession(Token), permission);
        }
        public bool CanDelete(String Token, int permission) {
            return CanDelete(GetSession(Token), permission);
        }
        public bool CanAuthorize(String Token, int permission) {
            return CanAuthorize(GetSession(Token), permission);
        }

        public void Remove(String Token) {
            var v = (from a in Sessions where a.Token == Token select a);
            if (v.Count() > 0) {
                foreach (UserSession s in v) {
                    Sessions.Remove(s);
                    break;
                }
            }
            // Here be DRAGONS!!
            if(DataAccessor is IRdbmsDataAccessor) {
                (DataAccessor as IRdbmsDataAccessor).Access(
                    (bd) => {
                        (bd).Execute($"UPDATE {typeof(TSession).Name.ToLower()} SET Ativo=b'0' WHERE Token=@1;", Token);
                    }
                );
            }
        }

        public IUser ForceCreateUser(string userName, string password) {
            TUser retv = new TUser();
            retv.Username = userName;
            retv.Password = AuthenticationUtils.HashPass(password, retv.RID);
            if(!DataAccessor.SaveItem(retv)) {
                throw new ValidationException(FTH.Strings.AUTH_USER_ALREADY_EXISTS);
            }
            return retv;
        }
        public IUser CreateUserSecure(string userName, string password, string confirmPassword) {
            if (password != confirmPassword) {
                throw new ValidationException(FTH.Strings.AUTH_PASSWORDS_MUST_MATCH);
            }
            return ForceCreateUser(userName, password);
        }

        public bool ForcePassword(IUser user, string newPassword) {
            user.Password = AuthenticationUtils.HashPass(newPassword, user.RID);
            DataAccessor.SaveItem(user);
            return true;
        }

        public bool ChangeUserPasswordSecure(IUser user, string oldPassword, string newPassword, string newPasswordConfirmation) {
            if(user.Password != AuthenticationUtils.HashPass(oldPassword, user.RID)) {
                throw new ValidationException(FTH.Strings.AUTH_PASSWORD_INCORRECT);
            }
            if (newPassword != newPasswordConfirmation) {
                throw new ValidationException(FTH.Strings.AUTH_PASSWORDS_MUST_MATCH);
            }
            return ForcePassword(user, newPassword);
        }
    }
}
