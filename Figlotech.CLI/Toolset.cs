using Figlotech.BDados;
using Figlotech.ConsoleUtils;
using System;

namespace Figlotech.CLI
{
    internal class Toolset
    {

        public static void CreateDbConfig(string path, string encryptionKey) {
            var database    = ConsoleEx.GetInfo<String>("Database");
            var host        = ConsoleEx.GetInfo<String>("Host da DB");
            var username    = ConsoleEx.GetInfo<String>("Usuario da DB");
            var password    = ConsoleEx.GetInfo<String>("Senha da DB");
            var poolsize    = ConsoleEx.GetInfo<int>("Max Conexoes");
            var dbcfg = new DataAccessorConfiguration() {
                Database = database,
                Host = host,
                User = username,
                Password = password,
                PoolSize = poolsize,
            };
        }
    }
}