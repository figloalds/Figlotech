using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using Figlotech.Data;

namespace Figlotech.BDados.Helpers {
    internal sealed class BDadosRestoreSession {
        private readonly IRdbmsDataAccessor _accessor;
        private readonly BDadosTransaction _transaction;
        private readonly BackupSqlDialect _dialect;
        private RestoreSessionState _previous;

        internal BDadosRestoreSession(IRdbmsDataAccessor accessor, BDadosTransaction transaction) {
            _accessor = accessor;
            _transaction = transaction;
            _dialect = accessor.QueryGenerator.BackupDialect;
        }

        internal static void RequireSupportedProvider(IQueryGenerator generator) {
            if (generator.BackupDialect is not (BackupSqlDialect.SQLite or BackupSqlDialect.MySQL or BackupSqlDialect.PostgreSQL)) {
                throw new NotSupportedException("The provider must explicitly support safe backup restoration.");
            }
        }

        internal async Task BeginAsync(IEnumerable<Type> types) {
            RequireSupportedProvider(_accessor.QueryGenerator);
            if (_transaction != null && !_transaction.IsUsingRdbmsTransaction) {
                throw new InvalidOperationException("Restoration requires an active database transaction.");
            }
            if (_dialect == BackupSqlDialect.SQLite) {
                // Unlike foreign_keys, this setting works inside a transaction and resets at its end.
                await _accessor.ExecuteAsync(_transaction, Qb.Fmt("PRAGMA defer_foreign_keys = ON;"));
            } else if (_dialect == BackupSqlDialect.PostgreSQL) {
                await _accessor.ExecuteAsync(_transaction, _accessor.QueryGenerator.DisableForeignKeysUntilTransactionEnd());
                foreach (var type in types) {
                    await _accessor.ExecuteAsync(_transaction, Qb.Fmt($"LOCK TABLE {_accessor.QueryGenerator.QuoteBackupIdentifier(type.Name)} IN SHARE ROW EXCLUSIVE MODE"));
                }
            } else {
                await RequireTransactionalTablesAsync(_accessor, _transaction, types);
                _previous = (await _accessor.QueryAsync<RestoreSessionState>(_transaction,
                    Qb.Fmt("SELECT @@SESSION.foreign_key_checks AS ForeignKeys, @@SESSION.sql_mode AS SqlMode"))).Single();
                string mode = string.Join(",", (_previous.SqlMode ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Concat(new[] { "STRICT_ALL_TABLES", "NO_AUTO_VALUE_ON_ZERO" }).Distinct(StringComparer.OrdinalIgnoreCase));
                await _accessor.ExecuteAsync(_transaction, Qb.Fmt("SET SESSION sql_mode = @mode", mode));
                await _accessor.ExecuteAsync(_transaction, _accessor.QueryGenerator.DisableForeignKeys());
            }
        }

        internal static async Task RequireTransactionalTablesAsync(IRdbmsDataAccessor accessor, BDadosTransaction transaction, IEnumerable<Type> types) {
            if (accessor.QueryGenerator.BackupDialect != BackupSqlDialect.MySQL) return;
            foreach (var type in types) {
                var rows = await accessor.QueryAsync<ValueBox<string>>(transaction,
                    Qb.Fmt("SELECT ENGINE AS Value FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @name", type.Name));
                if (rows.Count != 1 || !string.Equals(rows[0].Value, "InnoDB", StringComparison.OrdinalIgnoreCase)) {
                    throw new InvalidDataException($"Backup/restore requires an InnoDB table: '{type.Name}'.");
                }
            }
        }

        internal async Task EndAsync() {
            if (_previous == null) return;
            var query = Qb.Fmt("SET SESSION foreign_key_checks = @checks, SESSION sql_mode = @mode", _previous.ForeignKeys, _previous.SqlMode);
            try {
                if (_transaction != null) await _transaction.ExecuteRestoreCleanupAsync(query);
                else await _accessor.ExecuteAsync(_transaction, query);
            } catch (Exception cleanupError) {
                if (_transaction != null) {
                    try {
                        _transaction.DataAccessor.Plugin.DiscardConnection(_transaction.Connection);
                    } catch (Exception discardError) {
                        throw new AggregateException("Failed to restore and discard the database session.", cleanupError, discardError);
                    }
                }
                throw;
            }
        }

        internal async Task ValidateAsync(IEnumerable<Type> types) {
            if (_dialect == BackupSqlDialect.SQLite) {
                if ((await _accessor.QueryAsync<ValueBox<long>>(_transaction,
                    Qb.Fmt("SELECT COUNT(*) AS Value FROM pragma_foreign_key_check"))).Single().Value != 0) {
                    throw new InvalidDataException("Restored database contains foreign-key violations.");
                }
                return;
            }
            // Inspect actual constraints, including composite keys and constraints absent from CLR attributes.
            var columns = await _accessor.QueryAsync<RestoreForeignKey>(_transaction, ForeignKeyMetadata(_dialect));
            var names = new HashSet<string>(types.Select(t => t.Name), StringComparer.OrdinalIgnoreCase);
            foreach (var group in columns.GroupBy(c => c.ConstraintId)) {
                var first = group.First();
                if (!names.Contains(first.ChildTable) && !names.Contains(first.ParentTable)) continue;
                string child = Qualified(first.ChildSchema, first.ChildTable);
                string parent = Qualified(first.ParentSchema, first.ParentTable);
                string allPresent = string.Join(" AND ", group.Select(c => $"c.{Quote(c.ChildColumn)} IS NOT NULL"));
                string anyPresent = string.Join(" OR ", group.Select(c => $"c.{Quote(c.ChildColumn)} IS NOT NULL"));
                string matches = string.Join(" AND ", group.Select(c => $"p.{Quote(c.ParentColumn)} = c.{Quote(c.ChildColumn)}"));
                string invalid = $"(({allPresent}) AND NOT EXISTS (SELECT 1 FROM {parent} p WHERE {matches}))";
                if (first.MatchFull) invalid += $" OR (({anyPresent}) AND NOT ({allPresent}))";
                var query = Qb.Fmt($"SELECT COUNT(*) AS Value FROM (SELECT 1 FROM {child} c WHERE {invalid} LIMIT 1) violations");
                if ((await _accessor.QueryAsync<ValueBox<long>>(_transaction, query)).Single().Value != 0) {
                    throw new InvalidDataException($"Foreign-key violation in '{first.ChildSchema}.{first.ChildTable}' ({first.ConstraintId}).");
                }
            }
        }

        internal async Task SynchronizeSequenceAsync(BDadosRestoreBinding binding) {
            // SQLite and InnoDB advance their identity counters for explicit IDs automatically.
            if (_dialect != BackupSqlDialect.PostgreSQL) return;
            string table = _accessor.QueryGenerator.QuoteBackupIdentifier(binding.Type.Name);
            string key = _accessor.QueryGenerator.QuoteBackupIdentifier(binding.Key.Name);
            var sequences = await _accessor.QueryAsync<RestoreSequence>(_transaction, Qb.Fmt("""
                SELECT n.nspname AS SchemaName, c.relname AS SequenceName, s.seqincrement AS Increment,
                       s.seqmin AS Minimum, s.seqmax AS Maximum
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_sequence s ON s.seqrelid = c.oid
                WHERE c.oid = pg_get_serial_sequence(@table, @column)::regclass
                """, table, key.StartsWith('"') ? binding.Key.Name : binding.Key.Name.ToLowerInvariant()));
            foreach (var sequence in sequences) {
                if (sequence.Increment <= 0) throw new NotSupportedException("Restore only supports ascending PostgreSQL identity sequences.");
                string name = Qualified(sequence.SchemaName, sequence.SequenceName);
                var state = (await _accessor.QueryAsync<RestoreSequenceState>(_transaction,
                    Qb.Fmt($"SELECT last_value AS LastValue, is_called AS IsCalled FROM {name}"))).Single();
                var maximum = (await _accessor.QueryAsync<ValueBox<decimal?>>(_transaction,
                    Qb.Fmt($"SELECT MAX({key}) AS Value FROM {table}"))).Single().Value;
                decimal next = Math.Max(state.LastValue + (state.IsCalled ? (decimal)sequence.Increment : 0),
                    maximum.HasValue ? maximum.Value + sequence.Increment : sequence.Minimum);
                if (next > sequence.Maximum) throw new InvalidDataException($"Restored keys exhaust identity sequence '{name}'.");
                // ALTER RESTART is transactional, unlike setval; failures roll back sequence changes too.
                await _accessor.ExecuteAsync(_transaction, Qb.Fmt($"ALTER SEQUENCE {name} RESTART WITH {next.ToString(CultureInfo.InvariantCulture)}"));
            }
        }

        private string Quote(string name) => _dialect == BackupSqlDialect.MySQL
            ? "`" + name.Replace("`", "``") + "`" : "\"" + name.Replace("\"", "\"\"") + "\"";
        private string Qualified(string schema, string table) => Quote(schema) + "." + Quote(table);

        private static IQueryBuilder ForeignKeyMetadata(BackupSqlDialect dialect) => Qb.Fmt(dialect == BackupSqlDialect.MySQL ? """
            SELECT CONCAT(k.CONSTRAINT_SCHEMA, '.', k.TABLE_NAME, '.', k.CONSTRAINT_NAME) AS ConstraintId,
                   k.TABLE_SCHEMA AS ChildSchema, k.TABLE_NAME AS ChildTable, k.COLUMN_NAME AS ChildColumn,
                   k.REFERENCED_TABLE_SCHEMA AS ParentSchema, k.REFERENCED_TABLE_NAME AS ParentTable,
                   k.REFERENCED_COLUMN_NAME AS ParentColumn, 0 AS MatchFull
            FROM information_schema.KEY_COLUMN_USAGE k
            WHERE (k.TABLE_SCHEMA = DATABASE() OR k.REFERENCED_TABLE_SCHEMA = DATABASE()) AND k.REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION
            """ : """
            SELECT f.oid::text AS ConstraintId, cn.nspname AS ChildSchema, ct.relname AS ChildTable,
                   ca.attname AS ChildColumn, pn.nspname AS ParentSchema, pt.relname AS ParentTable,
                   pa.attname AS ParentColumn, (f.confmatchtype = 'f') AS MatchFull
            FROM pg_constraint f JOIN pg_class ct ON ct.oid = f.conrelid
            JOIN pg_namespace cn ON cn.oid = ct.relnamespace JOIN pg_class pt ON pt.oid = f.confrelid
            JOIN pg_namespace pn ON pn.oid = pt.relnamespace
            CROSS JOIN LATERAL unnest(f.conkey, f.confkey) AS keys(childkey, parentkey)
            JOIN pg_attribute ca ON ca.attrelid = ct.oid AND ca.attnum = keys.childkey
            JOIN pg_attribute pa ON pa.attrelid = pt.oid AND pa.attnum = keys.parentkey
            WHERE f.contype = 'f' AND (pg_table_is_visible(f.conrelid) OR pg_table_is_visible(f.confrelid))
            """);

        internal sealed class RestoreSessionState {
            public long ForeignKeys { get; set; }
            public string SqlMode { get; set; }
        }
        internal sealed class RestoreForeignKey {
            public string ConstraintId { get; set; }
            public string ChildSchema { get; set; }
            public string ChildTable { get; set; }
            public string ChildColumn { get; set; }
            public string ParentSchema { get; set; }
            public string ParentTable { get; set; }
            public string ParentColumn { get; set; }
            public bool MatchFull { get; set; }
        }
        internal sealed class RestoreSequence {
            public string SchemaName { get; set; }
            public string SequenceName { get; set; }
            public long Increment { get; set; }
            public long Minimum { get; set; }
            public long Maximum { get; set; }
        }
        internal sealed class RestoreSequenceState {
            public long LastValue { get; set; }
            public bool IsCalled { get; set; }
        }
    }
}
