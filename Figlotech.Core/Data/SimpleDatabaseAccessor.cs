using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Data
{
    public class SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>
        where TCommand : IDbCommand
        where TReader : IDataReader
        where TConnection : IDbConnection
    {
        TConnection Connection;
        public SimpleDatabaseAccessorQueryHelper(TConnection conn) {
            Connection = conn;
        }
        public T DbQuery<T>(Func<TCommand, T> fn) {
            using (var cmd = Connection.CreateCommand()) {
                return fn((TCommand)cmd);
            }
        }

        public Task<int> Execute(IQueryBuilder query) {
            return DbQuery(cmd => {
                query.ApplyToCommand(cmd);
                if (cmd is DbCommand cmdModern) {
                    return cmdModern.ExecuteNonQueryAsync();
                } else {
                    return Task.FromResult(
                        cmd.ExecuteNonQuery()
                    );
                }
            });
        }

        public Task<TReader> GetReader(IQueryBuilder query) {
            return DbQuery(cmd => {
                query.ApplyToCommand(cmd);
                if (cmd is DbCommand cmdModern) {
                    return cmdModern.ExecuteReaderAsync() as Task<TReader>;
                } else {
                    return Task.FromResult(
                        (TReader)cmd.ExecuteReader()
                    );
                }
            });
        }
        public Task<bool> HasAnyResults(IQueryBuilder query) {
            return DbQuery(async cmd => {
                query.ApplyToCommand(cmd);
                IDataReader reader;
                if (cmd is DbCommand cmdModern) {
                    reader = await cmdModern.ExecuteReaderAsync() as IDataReader;
                } else {
                    reader = cmd.ExecuteReader();
                }
                using (reader) {
                    return reader.Read();
                }
            });
        }

        public Task<List<Dictionary<string, object>>> GetResultAsDictionary(IQueryBuilder query) {
            return DbQuery(async cmd => {
                query.ApplyToCommand(cmd);
                var retv = new List<Dictionary<string, object>>();
                IDataReader reader;
                if (cmd is DbCommand cmdModern) {
                    reader = await cmdModern.ExecuteReaderAsync() as IDataReader;
                } else {
                    reader = cmd.ExecuteReader();
                }
                using (reader) {
                    var existingKeys = new string[reader.FieldCount];
                    for (int i = 0; i < reader.FieldCount; i++) {
                        existingKeys[i] = reader.GetName(i);
                    }
                    while (reader.Read()) {
                        var record = new Dictionary<string, object>();
                        for (int i = 0; i < existingKeys.Length; i++) {
                            record[existingKeys[i]] = reader.GetValue(i);
                        }
                        retv.Add(record);
                    }
                }

                return retv;
            });
        }

        public Task<List<T>> Query<T>(IQueryBuilder query) where T: new() {
            return DbQuery(async cmd => {
                query.ApplyToCommand(cmd);
                var retv = new List<T>();
                IDataReader reader;
                if (cmd is DbCommand cmdModern) {
                    reader = await cmdModern.ExecuteReaderAsync() as IDataReader;
                } else {
                    reader = cmd.ExecuteReader();
                }
                var propertiesOfT = typeof(T).GetProperties().Where(x=> x.GetSetMethod(false) != null).ToDictionary(t => t.Name.ToLower(), t=> t);
                using (reader) {
                    var existingKeys = new string[reader.FieldCount];
                    for (int i = 0; i < reader.FieldCount; i++) {
                        existingKeys[i] = reader.GetName(i).ToLower();
                    }
                    while (reader.Read()) {
                        var record = new T();
                        for (int i = 0; i < existingKeys.Length; i++) {
                            if(propertiesOfT.ContainsKey(existingKeys[i].ToLower())) {
                                var val = reader.GetValue(i);
                                var property = propertiesOfT[existingKeys[i].ToLower()];
                                if (val is DBNull) {
                                    val = null;
                                }
                                if(val != null || !property.PropertyType.IsValueType) {
                                    property.SetValue(record, val);
                                }
                            }
                        }
                        retv.Add(record);
                    }
                }

                return retv;
            });
        }
    }

    public class SimpleDatabaseAccessorBase<TConnection, TCommand, TReader>
        where TCommand : IDbCommand
        where TReader : IDataReader
        where TConnection : IDbConnection
    {
        Func<TConnection> GetConnectionCommand;
        public SimpleDatabaseAccessorBase(Func<TConnection> howToGetAConnection) {
            GetConnectionCommand = howToGetAConnection;
        }

        public async Task<T> Access<T>(Func<SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>, Task<T>> fn) {
            using (var connection = GetConnectionCommand()) {
                if (connection is DbConnection tasksSupportedConnection)
                    await tasksSupportedConnection.OpenAsync();
                else {
                    connection.Open();
                }
                return await fn(new SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>(connection));
            }
        }
        public async Task Access(Func<SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>, Task> fn) {
            using (var connection = GetConnectionCommand()) {
                if (connection is DbConnection tasksSupportedConnection)
                    await tasksSupportedConnection.OpenAsync();
                else {
                    connection.Open();
                }
                await fn(new SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>(connection));
            }
        }
        public async Task<T> Transact<T>(IsolationLevel isolationLevel, Func<SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>, Task<T>> fn) {
            using (var connection = GetConnectionCommand()) {
                if (connection is DbConnection tasksSupportedConnection)
                    await tasksSupportedConnection.OpenAsync();
                else {
                    connection.Open();
                }
                using (var transaction = connection.BeginTransaction(isolationLevel)) {
                    try {
                        var retv = await fn(new SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>(connection));
                        transaction.Commit();
                        return retv;
                    } catch (Exception x) {
                        transaction.Rollback();
                        throw x;
                    }
                }
            }
        }
        public async Task Transact(IsolationLevel isolationLevel, Func<SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>, Task> fn) {
            using (var connection = GetConnectionCommand()) {
                if (connection is DbConnection tasksSupportedConnection)
                    await tasksSupportedConnection.OpenAsync();
                else {
                    connection.Open();
                }
                using (var transaction = connection.BeginTransaction(isolationLevel)) {
                    try {
                        await fn(new SimpleDatabaseAccessorQueryHelper<TConnection, TCommand, TReader>(connection));
                        transaction.Commit();
                    } catch (Exception x) {
                        transaction.Rollback();
                        throw x;
                    }
                }
            }
        }
    }

    public class SimpleDatabaseAccessor : SimpleDatabaseAccessorBase<DbConnection, DbCommand, DbDataReader>
    {
        public SimpleDatabaseAccessor(Func<DbConnection> howToGetAConnection) : base(howToGetAConnection) {
        }
    }
}
