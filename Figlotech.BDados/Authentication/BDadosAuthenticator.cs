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

        public UserSession(BDadosAuthenticator auth) {
            Authenticator = auth;
        }

        public String UserRID;
        public String SessionRID;
        public String Username;
        public String Token;
        public DateTime Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));

        public void KeepAlive() {
            Expiry = DateTime.Now.Add(TimeSpan.FromMinutes(MaximumInactiveSpanInMinutes));
        }

        [JsonIgnore]
        protected BDadosAuthenticator Authenticator;

        public List<BDadosPermission> Permissions = new List<BDadosPermission>();

        public BDadosUser Usuario { get; set; }

        public void Logoff() {
            Authenticator.Logoff(this);
        }
    }

    public class BDadosAuthenticator : IRequiresDataAccessor {
        public IDataAccessor DataAccessor { get; set; }

        public BDadosAuthenticator(IDataAccessor dataAccessor) {

        }

        public BDadosAuthenticator(DependencySolver ds) {
            if (ds == null)
                return;
            ds.Resolve(this);
        }
        public BDadosAuthenticator() {
            DependencySolver.Default.Resolve(this);
        }

        public void CheckStructure(bool resetKeys = true) {
            if(DataAccessor is IRdbmsDataAccessor) {
                (DataAccessor as IRdbmsDataAccessor).CheckStructure(
                    new Type[] {
                        typeof(BDadosUser),
                        typeof(BDadosPermission)
                    }, resetKeys
                );
            }
        }

        public static List<BDadosAuthenticator> Authenticators = new List<BDadosAuthenticator>();
        private List<UserSession> WebBDadosUserSession = new List<UserSession>();
        public delegate bool MetodoLogin(UserSession s);
        private List<Attempt> TrackAttempts = new List<Attempt>();
        public long IdGlobal;
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

        private String New(BDadosUser Usuario, BDadosUserSession Session, String Token) {
            UserSession s = new UserSession(this);
            s.Token = Token;
            s.UserRID = Usuario.RID;
            s.SessionRID = Session.RID;
            s.Permissions = DataAccessor.LoadAll<BDadosPermission>((u) => u.User == Usuario.RID);
            WebBDadosUserSession.Add(s);
            return s.Token;
        }

        public BDadosUser CheckLogin(String userName, String password) {
            String criptPass = AuthenticationUtils.HashPass(password, userName.ToLower());
            var userQuery = DataAccessor.LoadAll<BDadosUser>(
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
            return userQuery.Any() ? userQuery.First() : null;
        }

        public String LoginUser(String Ip, LoginRequestModel Model) {
            var user = CheckLogin(Model.Username, Model.Password);
            if (user != null) {
                var sess = new BDadosUserSession {
                    User = user.RID,
                    isActive = true,
                    Token = FTH.GenerateIdString($"Login:{user.Username};"),
                    StartTime = DateTime.UtcNow,
                    EndTime = null,
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

        public void Logoff(UserSession s) {
            WebBDadosUserSession.Remove(s);
            if(DataAccessor is IRdbmsDataAccessor) {
                (DataAccessor as IRdbmsDataAccessor).Access((bd) => {
                    (bd).Execute("UPDATE BDadosUserSession SET Active=0 WHERE RID=@1", s.SessionRID);
                });
            } else {
                var session = DataAccessor.LoadByRid<BDadosUserSession>(s.SessionRID);
                session.isActive = false;
                DataAccessor.SaveItem(session);
            }
        }

        public UserSession GetSession(String Token) {
            var v = (from a in WebBDadosUserSession where a.Token == Token select a);
            if (v.Any())
                return v.First();
            else {
                var querySessao = DataAccessor.LoadAll<BDadosUserSession>((us) => us.Token.Equals(Token));
                if (querySessao.Any()) {
                    var User = DataAccessor.LoadByRid<BDadosUserSession>(querySessao.First().User) ?? new BDadosUserSession();
                    UserSession sess = new UserSession(this);
                    sess.Token = Token;
                    sess.UserRID = User.RID;
                    sess.SessionRID = querySessao.First().RID;
                    sess.Permissions = DataAccessor.LoadAll<BDadosPermission>((p) => p.User == User.RID);
                    WebBDadosUserSession.Add(sess);
                    return sess;
                }
            }
            return null;
        }

        public string Login(object interno, string ipCliente, LoginRequestModel modelo) {
            throw new NotImplementedException();
        }

        public void Remove(String Token) {
            var v = (from a in WebBDadosUserSession where a.Token == Token select a);
            if (v.Count() > 0) {
                foreach (UserSession s in v) {
                    WebBDadosUserSession.Remove(s);
                    break;
                }
            }
            if(DataAccessor is IRdbmsDataAccessor) {
                (DataAccessor as IRdbmsDataAccessor).Access(
                    (bd) => {
                        (bd).Execute($"UPDATE {nameof(BDadosUserSession)} SET Ativo=b'0' WHERE Token=@1", Token);
                    }
                );
            }
        }


    }
}
