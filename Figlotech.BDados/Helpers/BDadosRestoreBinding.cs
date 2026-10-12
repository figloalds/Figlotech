using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Figlotech.Data;

namespace Figlotech.BDados.Helpers {
    // One binding per table, reused by the ordinary accessor save path in RestorationMode.
    internal sealed class BDadosRestoreBinding {
        internal Type Type { get; }
        internal MemberInfo Key { get; }
        private readonly MemberInfo[] _members;
        private readonly IQueryGenerator _generator;
        private readonly Func<RdbmsDataAccessor, BDadosTransaction, IQueryBuilder, Task<IDataObject>> _readAsync;
        private readonly Func<RdbmsDataAccessor, BDadosTransaction, IQueryBuilder, IDataObject> _read;

        internal BDadosRestoreBinding(Type type, BDadosBackupCodec codec, IQueryGenerator generator) {
            Type = type;
            _generator = generator;
            _members = codec.RestoreMembers;
            var keys = BDadosBackupSchema.GetMembers(type).Where(m => m.IsDefined(typeof(PrimaryKeyAttribute), true)
                || m.GetCustomAttribute<FieldAttribute>(true).PrimaryKey).ToArray();
            if (keys.Length != 1 || !codec.HasSourceMember(keys[0])) {
                throw new InvalidDataException($"Restore requires one persisted primary key supplied by the archive for '{type.Name}'.");
            }
            Key = keys[0];
            var keyType = BDadosBackupSchema.GetMemberType(Key);
            if (BDadosBackupCodec.GetDataType(Nullable.GetUnderlyingType(keyType) ?? keyType) == BDadosBackupDataType.BinarySerializable) {
                throw new NotSupportedException("Custom binary primary keys need an explicit identity migration.");
            }
            _readAsync = typeof(BDadosRestoreBinding).GetMethod(nameof(ReadAsync), BindingFlags.Static | BindingFlags.NonPublic)
                .MakeGenericMethod(type).CreateDelegate<Func<RdbmsDataAccessor, BDadosTransaction, IQueryBuilder, Task<IDataObject>>>();
            _read = typeof(BDadosRestoreBinding).GetMethod(nameof(Read), BindingFlags.Static | BindingFlags.NonPublic)
                .MakeGenericMethod(type).CreateDelegate<Func<RdbmsDataAccessor, BDadosTransaction, IQueryBuilder, IDataObject>>();
        }

        internal object GetKey(IDataObject item) {
            object key = ReflectionTool.GetMemberValue(Key, item);
            if (key == null || !Equals(key, item.Id)) {
                throw new InvalidDataException($"Invalid or inconsistent primary key in '{Type.Name}'.");
            }
            return key;
        }

        internal IQueryBuilder Exists(IDataObject item) => Qb.Fmt(
            $"SELECT {_generator.QuoteBackupIdentifier(Key.Name)} AS Value FROM {_generator.QuoteBackupIdentifier(Type.Name)} WHERE {_generator.QuoteBackupIdentifier(Key.Name)} = @id", GetKey(item));

        internal bool ExistsWithoutCollision(List<ValueBox<object>> rows, IDataObject item) {
            if (rows.Count == 0) return false;
            var keyType = BDadosBackupSchema.GetMemberType(Key);
            object stored = BDadosBackupValueConverter.ConvertValue(rows.Single().Value, Nullable.GetUnderlyingType(keyType) ?? keyType);
            if (!SameValue(stored, GetKey(item))) {
                throw new InvalidDataException($"Destination key equality would merge different identities in '{Type.Name}'. Check the destination collation and key type.");
            }
            return true;
        }

        private IQueryBuilder ReadQuery(IDataObject item) => Qb.Fmt(
            $"SELECT {string.Join(", ", _members.Select(m => _generator.QuoteBackupIdentifier(m.Name)))} FROM {_generator.QuoteBackupIdentifier(Type.Name)} WHERE {_generator.QuoteBackupIdentifier(Key.Name)} = @id", GetKey(item));

        internal async Task VerifyAsync(RdbmsDataAccessor accessor, BDadosTransaction transaction, IDataObject item) {
            Verify(item, await _readAsync(accessor, transaction, ReadQuery(item)));
        }
        internal void Verify(RdbmsDataAccessor accessor, BDadosTransaction transaction, IDataObject item) {
            Verify(item, _read(accessor, transaction, ReadQuery(item)));
        }
        private void Verify(IDataObject expected, IDataObject actual) {
            foreach (var member in _members) {
                if (!SameValue(ReflectionTool.GetMemberValue(member, expected), ReflectionTool.GetMemberValue(member, actual))) {
                    throw new InvalidDataException($"Database changed restored value '{Type.Name}.{member.Name}'. Check column precision, type and triggers.");
                }
            }
        }
        private static bool SameValue(object left, object right) {
            if (left is byte[] a && right is byte[] b) return a.AsSpan().SequenceEqual(b);
            if (left is IBinarySerializable x && right is IBinarySerializable y) return x.ToBytes().AsSpan().SequenceEqual(y.ToBytes());
            if (left is DateTimeOffset date && right is DateTimeOffset other) return date.EqualsExact(other);
            return Equals(left, right);
        }
        private static async Task<IDataObject> ReadAsync<T>(RdbmsDataAccessor accessor, BDadosTransaction transaction, IQueryBuilder query) where T : IDataObject, new() {
            return (await accessor.QueryAsync<T>(transaction, query)).Single();
        }
        private static IDataObject Read<T>(RdbmsDataAccessor accessor, BDadosTransaction transaction, IQueryBuilder query) where T : IDataObject, new() {
            return accessor.Query<T>(transaction, query).Single();
        }

        internal IQueryBuilder Save(IDataObject item, bool update) {
            if (item.GetType() != Type) throw new InvalidOperationException("Restore binding does not match the saved model.");
            string table = _generator.QuoteBackupIdentifier(Type.Name);
            var members = update ? _members.Where(m => m != Key).ToArray() : _members;
            if (update && members.Length == 0) return null;
            var query = new QueryBuilder(update ? $"UPDATE {table} SET " : $"INSERT INTO {table} (");
            if (!update) {
                query.Append(string.Join(", ", members.Select(m => _generator.QuoteBackupIdentifier(m.Name))) + ")");
                if (_generator.BackupDialect == BackupSqlDialect.PostgreSQL) query.Append(" OVERRIDING SYSTEM VALUE");
                query.Append(" VALUES (");
            }
            for (int i = 0; i < members.Length; i++) {
                if (i > 0) query.Append(", ");
                if (update) query.Append(_generator.QuoteBackupIdentifier(members[i].Name) + " = ");
                query.Append($"@value{i}", ReflectionTool.GetMemberValue(members[i], item));
            }
            if (update) query.Append($" WHERE {_generator.QuoteBackupIdentifier(Key.Name)} = @id", GetKey(item));
            else query.Append(")");
            return query;
        }
    }
}
