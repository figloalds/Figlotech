using Figlotech.Core;
using Figlotech.Core.Autokryptex;
using Newtonsoft.Json;
using System;
using System.IO;

namespace Figlotech.BDados.SqliteDataAccessor {

    public sealed class SqlitePluginConfiguration {
        public String DataSource { get; set; }
        public String Schema { get; set; }
        public int PoolSize { get; set; } = 100;

        public void SaveToFile(String path, String password) {
            var json = JsonConvert.SerializeObject(this);
            var bytes = Fi.StandardEncoding.GetBytes(json);
            var autokryptex = new TheAutoEncryptorV1(password, 4);
            var encryptedBytes = autokryptex.Encrypt(bytes);

            File.WriteAllBytes(path, encryptedBytes);
        }
        public static SqlitePluginConfiguration LoadFromFile(String path, String password) {
            if (!File.Exists(path)) {
                return null;
            }

            var bytes = File.ReadAllBytes(path);
            var autokryptex = new TheAutoEncryptorV1(password, 4);
            var decryptedBytes = autokryptex.Decrypt(bytes);
            var json = Fi.StandardEncoding.GetString(decryptedBytes);
            var obj = JsonConvert.DeserializeObject<SqlitePluginConfiguration>(json);

            return obj;
        }

        public String GetConnectionString() {
            // Microsoft.Data.Sqlite does not support the 'Max Pool Size' keyword; pooling is
            // controlled by the 'Pooling' keyword alone and the ADO.NET pool has no size cap here.
            return $"Data Source={DataSource};Pooling=True;";
        }

    }
}
