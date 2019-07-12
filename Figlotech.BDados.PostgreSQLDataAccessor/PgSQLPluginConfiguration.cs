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

namespace Figlotech.BDados.PgSQLDataAccessor
{

    public class PgSQLPluginConfiguration {
        public String Host { get; set; }
        public String Database { get; set; }
        public String User { get; set; }
        public String Password { get; set; }
        public String Schema { get; set; }
        public int Port { get; set; } = 5432;
        public int PoolSize { get; set; } = 10;
        public bool UsePooling { get; set; } = true;
        public int Timeout { get; set; } = 1024;
        public int Lifetime { get; set; } = 1024;
        public bool ResetConnection { get; set; } = true;
        public bool ContinuousConnection { get; set; } = false;

        public void SaveToFile(String path, String password) {

            var json = JsonConvert.SerializeObject(this);
            var bytes = Fi.StandardEncoding.GetBytes(json);
            var autokryptex = new TheAutoEncryptorV1(password, 429497291);
            var encryptedBytes = autokryptex.Encrypt(bytes);

            File.WriteAllBytes(path, encryptedBytes);
        }
        public static PgSQLPluginConfiguration LoadFromFile(String path, String password) {
            if(!File.Exists(path)) {
                return null;
            }

            var bytes = File.ReadAllBytes(path);
            var autokryptex = new TheAutoEncryptorV1(password, 429497291);
            var decryptedBytes = autokryptex.Decrypt(bytes);
            var json = Fi.StandardEncoding.GetString(decryptedBytes);
            var obj = JsonConvert.DeserializeObject<PgSQLPluginConfiguration>(json);

            return obj;
        }

        public String GetConnectionString() {
            var config = new Npgsql.NpgsqlConnectionStringBuilder() {
                Host = Host,
                Port = Port,
                Username = User,
                Password = Password,
                PersistSecurityInfo = true,
                Database = Database??"",
                MinPoolSize = 1,
                MaxPoolSize = PoolSize,
                Pooling = UsePooling,
                Timeout = Timeout,
                ConnectionIdleLifetime = Lifetime,
                CommandTimeout = Timeout,
            };
            return config.ToString();
        }

    }
}
