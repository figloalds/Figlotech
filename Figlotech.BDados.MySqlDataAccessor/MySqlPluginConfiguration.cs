using System;
using Microsoft.Win32;
using System.Configuration;
using System.Diagnostics;
using Figlotech.Core.Autokryptex;
using Newtonsoft.Json;
using System.Text;
using System.IO;
using Figlotech.Core.Autokryptex.EncryptMethods;

namespace Figlotech.BDados.MySqlDataAccessor
{

    public class MySqlPluginConfiguration {
        public String Host { get; set; }
        public String Database { get; set; }
        public String User { get; set; }
        public String Password { get; set; }
        public int Port { get; set; } = 3306;
        public int PoolSize { get; set; } = 10;
        public bool UsePooling { get; set; } = true;
        public int Timeout { get; set; } = 60000;
        public int Lifetime { get; set; } = 60000;
        public bool ResetConnection { get; set; } = true;
        public bool ContinuousConnection { get; set; } = false;
        public bool RequireSSL { get; set; } = false;

        public void SaveToFile(String path, String password) {

            var json = JsonConvert.SerializeObject(this);
            var bytes = Encoding.UTF8.GetBytes(json);
            var autokryptex = new AutokryptexEncryptor(password);
            var encryptedBytes = autokryptex.Encrypt(bytes);

            File.WriteAllBytes(path, encryptedBytes);
        }
        public static MySqlPluginConfiguration LoadFromFile(String path, String password) {
            if(!File.Exists(path)) {
                return null;
            }

            var bytes = File.ReadAllBytes(path);
            var autokryptex = new AutokryptexEncryptor(password);
            var decryptedBytes = autokryptex.Decrypt(bytes);
            var json = Encoding.UTF8.GetString(decryptedBytes);
            var obj = JsonConvert.DeserializeObject<MySqlPluginConfiguration>(json);

            return obj;
        }

        public String GetConnectionString() { 
            return $"server={Host};port={Port};user id={User};password={Password};persistsecurityinfo=True;{(Database != null ? $"database={Database}" : "")};Min Pool Size=1;Max Pool Size={PoolSize};Pooling={UsePooling};ConnectionTimeout={Timeout};DefaultCommandTimeout={Timeout};ConnectionLifetime={Lifetime};ConnectionReset={ResetConnection};Allow User Variables=True;Convert Zero Datetime=True;SSL Mode={(RequireSSL?"Required":"None")};CharSet=utf8;";
        }

    }
}
