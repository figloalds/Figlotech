using System;
using Microsoft.Win32;
using System.Configuration;
using System.Diagnostics;
using Figlotech.Core.Autokryptex;
using Newtonsoft.Json;
using System.Text;
using System.IO;
using Figlotech.Core.Autokryptex.EncryptMethods;

namespace Figlotech.BDados.SqliteDataAccessor
{

    public class SqlitePluginConfiguration {
        public String DataSource { get; set; }
        public String Schema { get; set; }
        public int PoolSize { get; set; } = 100;

        public void SaveToFile(String path, String password) {
            var json = JsonConvert.SerializeObject(this);
            var bytes = Encoding.UTF8.GetBytes(json);
            var autokryptex = new AutokryptexEncryptor(password);
            var encryptedBytes = autokryptex.Encrypt(bytes);

            File.WriteAllBytes(path, encryptedBytes);
        }
        public static SqlitePluginConfiguration LoadFromFile(String path, String password) {
            if(!File.Exists(path)) {
                return null;
            }

            var bytes = File.ReadAllBytes(path);
            var autokryptex = new AutokryptexEncryptor(password);
            var decryptedBytes = autokryptex.Decrypt(bytes);
            var json = Encoding.UTF8.GetString(decryptedBytes);
            var obj = JsonConvert.DeserializeObject<SqlitePluginConfiguration>(json);

            return obj;
        }

        public String GetConnectionString() { 
            return $"Data Source={DataSource};Pooling=True;Max Pool Size={PoolSize};";
        }

    }
}
