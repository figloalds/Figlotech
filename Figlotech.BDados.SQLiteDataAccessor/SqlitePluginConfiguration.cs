﻿using System;
using Microsoft.Win32;
using System.Configuration;
using System.Diagnostics;
using Figlotech.Core.Autokryptex;
using Newtonsoft.Json;
using System.Text;
using System.IO;
using Figlotech.Core.Autokryptex.EncryptMethods;
using Figlotech.Core;

namespace Figlotech.BDados.SqliteDataAccessor
{

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
            if(!File.Exists(path)) {
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
            return $"Data Source={DataSource};Pooling=True;Max Pool Size={PoolSize};";
        }

    }
}
