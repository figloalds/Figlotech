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

    public class LoginRequestModel {
        /// <summary>
        /// <para>Id do captcha solicidado (esses captchas tem um tempo limite de 30 minutos)</para>
        /// </summary>
        public String FormId { get; set; }
        /// <summary>
        /// <para>Nome de usuario</para>
        /// </summary>
        public String Username { get; set; }
        /// <summary>
        /// <para>Senha do usuario</para>
        /// </summary>
        public String Password { get; set; }
        /// <summary>
        /// <para>Resolução do captcha</para>
        /// </summary>
        public String Captcha { get; set; }
    }

    public class Attempt {
        public String User;
        public bool Lock = false;
        public int Attempts = 0;
        public DateTime LockStamp;

        public const int maxAttemptsToLock = 7;
        public static TimeSpan lockTime = TimeSpan.FromMinutes(1);
    }

    public class UserSession {
        public static int MaximumInactiveSpanInMinutes = 60;

        public UserSession(IBDadosAuthenticator auth) {
            Authenticator = auth;
        }

        public String UserRID;
        public IBDadosUserSession SessionObject;
        public String Username;
        public String Token;
        public DateTime Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));

        public void KeepAlive() {
            Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));
        }

        [JsonIgnore]
        protected IBDadosAuthenticator Authenticator;

        public IList<IBDadosPermission> Permissions;

        public IBDadosUser User { get; set; }

        public void Logoff() {
            Authenticator.Logoff(this.Token);
        }
    }

    public class BDadosAuthenticator<TUser, TPermission, TSession>
        : IBDadosAuthenticator, IRequiresDataAccessor
        where TUser: IBDadosUser, new()
        where TPermission: IBDadosPermission, new()
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

        public Attempt TrackAttempt(String userName, bool Success) {
            var att = TrackAttempts.Find((a) => a.User == userName) ?? new Attempt();
            if (Success) return att;
            if (att != null) {
                if (att.Lock) {
                    if (DateTime.UtcNow.Subtract(att.LockStamp).TotalMilliseconds > Attempt.lockTime.TotalMilliseconds) {
                        att.Lock = false;
                        att.Attempts = 0;
                    }
                }
                else if (++att.Attempts > Attempt.maxAttemptsToLock) {
                    att.Lock = true;
                    att.LockStamp = DateTime.UtcNow;
                }
            }
            else {
                att = new Attempt { User = userName };
            }

            return att;
        }

        private String New(IBDadosUser User, TSession Session, String Token) {
            UserSession s = new UserSession(this);
            s.Token = Token;
            s.UserRID = User.RID;
            s.SessionObject = Session;
            s.Permissions = (IList<IBDadosPermission>) DataAccessor.LoadAll<TPermission>((u) => u.User == User.RID);
            Sessions.Add(s);
            return s.Token;
        }

        public TUser CheckLogin(String userName, String password) {
            String criptPass = AuthenticationUtils.HashPass(password, userName.ToLower());
            var userQuery = DataAccessor.LoadAll<TUser>(
                (u) => u.Username == userName);
            if (!userQuery.Any()) {
                TrackAttempt(userName, false);
                throw new UserNotRegisteredException("User is not registered.");
            }
            var loadedUser = userQuery.First();
            if(loadedUser.Password != criptPass) {
                throw new PasswordIncorrectException("Password provided is incorrect");
            }
            if (!loadedUser.isActive) {
                throw new UserBlockedException("User is blocked.");
            }
            return userQuery.FirstOrDefault();
        }

        public String Login(String Username, String Password) {
            var user = CheckLogin(Username, Password);
            if (user != null) {
                var li = DataAccessor.LoadAll<TPermission>(p => p.User == user.RID);
                var sess = new TSession {
                    User = user.RID,
                    isActive = true,
                    Token = FTH.GenerateIdString($"Login:{user.Username};"),
                    StartTime = DateTime.UtcNow,
                    EndTime = null,
                    Permissions = li.Select(p=>(IBDadosPermission) p).ToList()
                };
                if (DataAccessor.SaveItem(sess)) {
                    return New(user, sess, sess.Token);
                }
            }
            return null;
        }

        public bool Exists(String Token) {
            return GetSession(Token) != null;
        }

        public void Logoff(String s) {
            Sessions.RemoveAll(a=> a.Token == s);
            if(DataAccessor is IRdbmsDataAccessor) {
                (DataAccessor as IRdbmsDataAccessor).Access((bd) => {
                    // DID ANYONE SAY DRAGONS?!
                    (bd).Execute($"UPDATE {typeof(TSession).Name.ToLower()} SET Active=0 WHERE Token=@1", s);
                });
            } else {
                var session = DataAccessor.LoadByRid<TUser>(s);
                session.isActive = false;
                DataAccessor.SaveItem(session);
            }
        }

        public IBDadosUserSession GetSession(String Token) {
            var v = (from a in Sessions where a.Token == Token select a);
            if (v.Any())
                return v.First().SessionObject;
            else {
                var fetchSession = DataAccessor.LoadAll<TSession>((us) => us.Token.Equals(Token));
                if (fetchSession.Any()) {
                    var User = DataAccessor.LoadByRid<TUser>(fetchSession.First().User);
                    UserSession sess = new UserSession(this);
                    sess.Token = Token;
                    sess.UserRID = User.RID;
                    sess.SessionObject = fetchSession.First();
                    sess.Permissions = (IList<IBDadosPermission>) DataAccessor.LoadAll<TPermission>((p) => p.User == User.RID);
                    var retv = fetchSession.FirstOrDefault();
                    retv.Permissions = DataAccessor
                        .LoadAll<TPermission>(p => p.User == User.RID)
                        .Select(p=>(IBDadosPermission) p)
                        .ToList();
                    Sessions.Add(sess);
                    return fetchSession.FirstOrDefault();
                }
            }
            return null;
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
