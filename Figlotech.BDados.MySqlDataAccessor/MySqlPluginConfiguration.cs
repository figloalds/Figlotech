using System;
using Microsoft.Win32;
using System.Configuration;
using System.Diagnostics;
using Figlotech.Core.Autokryptex;
using Newtonsoft.Json;
using System.Text;
using System.IO;
using Figlotech.Core.Autokryptex.EncryptMethods;
using Figlotech.Core;
using Figlotech.Core.Autokryptex.Legacy;

namespace Figlotech.BDados.MySqlDataAccessor
{

    public sealed class MySqlPluginConfiguration {
        public String Host { get; set; }
        public String Database { get; set; }
        public String User { get; set; }
        public String Password { get; set; }
        public int Port { get; set; } = 3306;
        public int PoolSize { get; set; } = 10;
        public bool UsePooling { get; set; } = true;
        public int Timeout { get; set; } = 60000;
        public int ConnectTimeout { get; set; } = 30;
        public int Lifetime { get; set; } = 60000;
        public bool ResetConnection { get; set; } = true;
        public bool ContinuousConnection { get; set; } = false;
        public int DefaultCommandTimeout { get; set; } = 600;
        public bool RequireSSL { get; set; } = false;

        public void SaveToFile(String path, String password) {

            var json = JsonConvert.SerializeObject(this);
            var bytes = Fi.StandardEncoding.GetBytes(json);
            var autokryptex = new TheAutoEncryptorV1(password, 429497291);
            var encryptedBytes = autokryptex.Encrypt(bytes);

            File.WriteAllBytes(path, encryptedBytes);
        }
        public static MySqlPluginConfiguration LoadFromFile(String path, String password) {
            if(!File.Exists(path)) {
                return null;
            }

            var bytes = File.ReadAllBytes(path);
            var autokryptex = new TheAutoEncryptorV1(password, 429497291);
            var decryptedBytes = autokryptex.Decrypt(bytes);
            var json = Fi.StandardEncoding.GetString(decryptedBytes);
            var obj = JsonConvert.DeserializeObject<MySqlPluginConfiguration>(json);

            return obj;
        }

        public String GetConnectionString() { 
            var connstr = $"server={Host};port={Port};user id={User};password={Password};persistsecurityinfo=True;{(Database != null ? $"database={Database};" : "")}Min Pool Size=1;Max Pool Size={PoolSize};Pooling={UsePooling};ConnectionTimeout={Timeout};Connect Timeout={ConnectTimeout};DefaultCommandTimeout={Timeout};ConnectionLifetime={Lifetime};ConnectionReset={ResetConnection};Allow User Variables=True;Convert Zero Datetime=True;SSL Mode={(RequireSSL?"Required":"None")};charset=utf8mb4;default command timeout={DefaultCommandTimeout};AllowPublicKeyRetrieval=true;";
            return connstr;
        }
    }
}
