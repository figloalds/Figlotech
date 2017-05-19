using Figlotech.BDados.Authentication.Exceptions;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.Requirements;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        public UserSession(IBDadosAuthenticator auth) {
            Authenticator = auth;
        }

        public String UserRID;
        public IBDadosUserSession Session;
        public String Username;
        public String Token;
        public DateTime Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));

        public void KeepAlive() {
            Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));
        }

        [JsonIgnore]
        protected IBDadosAuthenticator Authenticator;

        public IBDadosPermissionsContainer Permissions;

        public IBDadosUser User { get; set; }

        public void Logoff() {
            Authenticator.Logoff(Session);
        }
    }

    public class BDadosAuthenticator<TUser, TSession>
        : IBDadosAuthenticator, IRequiresDataAccessor
        where TUser: IBDadosUser, new()
        where TSession: IBDadosUserSession, new() {
        public IDataAccessor DataAccessor { get; set; }

        public BDadosAuthenticator(IDataAccessor dataAccessor) {
            DataAccessor = dataAccessor;
        }

        public BDadosAuthenticator(DependencySolver ds = null) {
            if (ds == null)
                return;
            ds.Resolve(this);
        }
        public BDadosAuthenticator() {
            DependencySolver.Default.Resolve(this);
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
                    throw new UserBlockedException(String.Format(FTH.Strings.AUTH_USER_MAX_ATTEMPTS_EXCEEDED, Attempt.maxAttemptsToLock));
                }
            }
        }

        private String New(IBDadosUser User, TSession Session, String Token) {
            UserSession s = new UserSession(this);
            s.Token = Token;
            s.UserRID = User.RID;
            s.Session = Session;
            s.Permissions = User.GetPermissionsContainer();
            Sessions.Add(s);
            return s.Token;
        }

        public TUser CheckLogin(String userName, String password) {
            String criptPass = AuthenticationUtils.HashPass(password, userName.ToLower());
            var userQuery = DataAccessor.LoadAll<TUser>(
                (u) => u.Username == userName);
            if (!userQuery.Any()) {
                TrackAttempt(userName, false);
                throw new UserNotRegisteredException(FTH.Strings.AUTH_USER_NOT_FOUND);
            }
            var loadedUser = userQuery.First();
            if(loadedUser.Password != criptPass) {
                throw new PasswordIncorrectException(FTH.Strings.AUTH_PASSWORD_INCORRECT);
            }
            if (!loadedUser.isActive) {
                throw new UserBlockedException(FTH.Strings.AUTH_USER_BLOCKED);
            }
            return userQuery.FirstOrDefault();
        }

        public IBDadosUserSession Login(String Username, String Password) {
            var user = CheckLogin(Username, Password);
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

        public void Logoff(IBDadosUserSession s) {
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

        public IBDadosUserSession GetSession(String Token) {
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

        public bool CanCreate(IBDadosUserSession Session, int permission) {
            return Session?.CanCreate(permission) ?? false;
        }
        public bool CanRead(IBDadosUserSession Session, int permission) {
            return Session?.CanRead(permission) ?? false;
        }
        public bool CanUpdate(IBDadosUserSession Session, int permission) {
            return Session?.CanUpdate(permission) ?? false;
        }
        public bool CanDelete(IBDadosUserSession Session, int permission) {
            return Session?.CanDelete(permission) ?? false;
        }
        public bool CanAuthorize(IBDadosUserSession Session, int permission) {
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


    }
}
