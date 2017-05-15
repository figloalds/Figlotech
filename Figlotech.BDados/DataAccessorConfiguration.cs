using System;
using Microsoft.Win32;
using System.Configuration;
using System.Diagnostics;
using Figlotech.Autokryptex;

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

        public String ConnectionString { 
            get {
                return $"server={Host};port={Port};user id={User};password={Password};persistsecurityinfo=True;{(Database != null ? $"database={Database}" : "")};Max Pool Size={PoolSize};Pooling={UsePooling};ConnectionTimeout={Timeout};DefaultCommandTimeout={Timeout};DefaultTableCacheAge={Timeout};ConnectionLifetime={Lifetime};ConnectionReset={ResetConnection};Allow User Variables=True;Convert Zero Datetime=True;";
            }
        }

    }
}
