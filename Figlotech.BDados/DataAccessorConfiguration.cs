using System;
using Microsoft.Win32;
using System.Configuration;
using System.Diagnostics;
using Figlotech.Autokryptex;
using Newtonsoft.Json;
using System.Text;
using System.IO;
using Figlotech.Autokryptex.EncryptMethods;

namespace Figlotech.BDados
{
    public enum DataProvider
    {
        MySql,
        SqlServer,
        OracleDB,
        Firebird,
    }

    public class DataAccessorConfiguration
    {
        public String Host;
        public String Database;
        public String User;
        public String Password;
        public int Port = 3306;
        public int PoolSize = 10;
        public bool UsePooling = true;
        public DataProvider Provider;
        public int Timeout = 60000;
        public int Lifetime = 60000;
        public bool ResetConnection = false;
        public bool ContinuousConnection = false;

        public void SaveToFile(String path, String password) {

            var json = JsonConvert.SerializeObject(this);
            var bytes = Encoding.UTF8.GetBytes(json);
            var autokryptex = new AutokryptexEncryptor(password);
            var encryptedBytes = autokryptex.Encrypt(bytes);

            using (FileStream fs = File.Open(path, FileMode.Create)) {
                fs.Write(encryptedBytes, 0, encryptedBytes.Length);
            }
        }
        public static DataAccessorConfiguration LoadFromFile(String path, String password) {

            var bytes = File.ReadAllBytes(path);
            var autokryptex = new AutokryptexEncryptor(password);
            var decryptedBytes = autokryptex.Decrypt(bytes);
            var json = Encoding.UTF8.GetString(bytes);
            var obj = JsonConvert.DeserializeObject<DataAccessorConfiguration>(json);

            return obj;
        }

        public String GetConnectionString() { 
            return $"server={Host};port={Port};user id={User};password={Password};persistsecurityinfo=True;{(Database != null ? $"database={Database}" : "")};Max Pool Size={PoolSize};Pooling={UsePooling};ConnectionTimeout={Timeout};DefaultCommandTimeout={Timeout};DefaultTableCacheAge={Timeout};ConnectionLifetime={Lifetime};ConnectionReset={ResetConnection};Allow User Variables=True;Convert Zero Datetime=True;";
        }

    }
}
