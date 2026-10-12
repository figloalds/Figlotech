using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core.Interfaces;
using Figlotech.Core;
using Figlotech.Data;

namespace Figlotech.BDados.Helpers {
    /// <summary>
    /// Versioned binary backups of explicitly selected BDados models. Caller streams remain open.
    /// Legacy APIs support forward-only streams; appendable container APIs require seeking.
    /// Restore uses the accessor's transaction-scoped restoration mode to preserve stored values.
    /// </summary>
    public sealed partial class BDadosBackup {
        private static readonly byte[] Magic = "FGBACKUP"u8.ToArray();
        private static readonly byte[] EndMagic = "FGBEND01"u8.ToArray();
        private static readonly MethodInfo FetchMethod = typeof(BDadosBackup).GetMethod(nameof(FetchRowsAsync), BindingFlags.NonPublic | BindingFlags.Instance);
        private static readonly MethodInfo SaveChunkMethod = typeof(BDadosBackup).GetMethod(nameof(SaveRestoreChunkAsync), BindingFlags.NonPublic | BindingFlags.Instance);
        private readonly IRdbmsDataAccessor _accessor;
        public int MaxHeaderBytes { get; }
        public int MaxRowBytes { get; }
        public BDadosBackupOptions Options { get; }

        public BDadosBackup(IRdbmsDataAccessor accessor, int maxHeaderBytes = 16 * 1024 * 1024, int maxRowBytes = 64 * 1024 * 1024)
            : this(accessor, null, maxHeaderBytes, maxRowBytes) {
        }

        public BDadosBackup(IRdbmsDataAccessor accessor, BDadosBackupOptions options,
            int maxHeaderBytes = 16 * 1024 * 1024, int maxRowBytes = 64 * 1024 * 1024) {
            ArgumentNullException.ThrowIfNull(accessor);
            if (maxHeaderBytes < 4) throw new ArgumentOutOfRangeException(nameof(maxHeaderBytes));
            if (maxRowBytes < 4) throw new ArgumentOutOfRangeException(nameof(maxRowBytes));
            _accessor = accessor;
            MaxHeaderBytes = maxHeaderBytes;
            MaxRowBytes = maxRowBytes;
            Options = options ?? new BDadosBackupOptions();
        }

        public static BDadosBackupSchema CreateSchema<T>() => BDadosBackupSchema.Create<T>();
        public static BDadosBackupSchema CreateSchema(Type type) => BDadosBackupSchema.Create(type);

        /// <summary>Exports field values using the schema's framing; no schema or names are included.</summary>
        public static byte[] Export(object instance, BDadosBackupSchema schema) {
            ArgumentNullException.ThrowIfNull(instance);
            return new BDadosBackupCodec(instance.GetType(), schema).Export(instance);
        }

        public static T Import<T>(BDadosBackupSchema schema, byte[] bytes) where T : class, new() {
            return (T)Import(typeof(T), schema, bytes);
        }

        public static object Import(Type type, BDadosBackupSchema schema, byte[] bytes) {
            return new BDadosBackupCodec(type, schema, forImport: true).Import(bytes);
        }

        /// <summary>
        /// Writes tables in the supplied order, fetching all rows without the accessor's default query limit.
        /// Choose dependency order if the destination enforces foreign keys during restore.
        /// </summary>
        public Task BackupAsync(Stream destination, IEnumerable<Type> tableTypes, CancellationToken cancellationToken = default,
            IsolationLevel isolationLevel = IsolationLevel.Serializable) {
            return BackupCoreAsync(destination, tableTypes, null, cancellationToken, isolationLevel);
        }

        /// <summary>Writes and flushes a sibling temporary file before publishing the completed archive.</summary>
        public async Task BackupFileAsync(string path, IEnumerable<Type> tableTypes, CancellationToken cancellationToken = default,
            IsolationLevel isolationLevel = IsolationLevel.Serializable, bool overwrite = false) {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
            string target = Path.GetFullPath(path);
            string temporary = Path.Combine(Path.GetDirectoryName(target), $".{Path.GetFileName(target)}.{Guid.NewGuid():N}.partial");
            try {
                await using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536, FileOptions.Asynchronous)) {
                    await BackupAsync(stream, tableTypes, cancellationToken, isolationLevel);
                    await stream.FlushAsync(cancellationToken);
                    stream.Flush(flushToDisk: true);
                }
                cancellationToken.ThrowIfCancellationRequested();
                File.Move(temporary, target, overwrite);
            } finally {
                if (File.Exists(temporary)) File.Delete(temporary);
            }
        }

        /// <summary>
        /// Writes a self-contained incremental containing rows whose CreatedAt or UpdatedAt is
        /// strictly greater than since. Timestamp attributes override the conventional member names.
        /// Each table schema is written once, including tables with no matching rows. RestoreAsync
        /// reads the resulting archive normally. Deletions are not tracked and the cutoff is not
        /// converted between time zones; use the same time convention as the source database.
        /// </summary>
        public Task BackupIncrementalAsync(Stream destination, IEnumerable<Type> tableTypes, DateTime since,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable) {
            return BackupCoreAsync(destination, tableTypes, since, cancellationToken, isolationLevel);
        }

        private async Task BackupCoreAsync(Stream destination, IEnumerable<Type> tableTypes, DateTime? since,
            CancellationToken cancellationToken, IsolationLevel isolationLevel) {
            ArgumentNullException.ThrowIfNull(destination);
            if (!destination.CanWrite) throw new ArgumentException("The backup stream must be writable.", nameof(destination));
            cancellationToken.ThrowIfCancellationRequested();
            var types = GetTableTypes(tableTypes);
            var schemas = types.Select(BDadosBackupSchema.Create).ToArray();
            var codecs = types.Select((type, i) => new BDadosBackupCodec(type, schemas[i])).ToArray();
            // Resolve and validate every predicate before writing any archive bytes.
            var conditions = types.Select(type => since.HasValue ? CreateIncrementalCondition(type, since.Value) : null).ToArray();
            byte[] header = CreateHeader(schemas);
            using var wire = new Wire(destination, cancellationToken);
            await wire.WriteAsync(Magic);
            await wire.WriteInt32Async(1);
            await wire.WriteInt32Async(header.Length);
            await wire.WriteAsync(header);
            await wire.WriteAsync(SHA256.HashData(header));
            await _accessor.AccessAsync(async transaction => {
                if (transaction != null) {
                    transaction.BackupMode = true;
                    transaction.Benchmarker = new Benchmarker("Backup") { Active = false };
                }
                await BDadosRestoreSession.RequireTransactionalTablesAsync(_accessor, transaction, types);
                for (int i = 0; i < types.Length; i++) {
                    long count = 0;
                    var rows = (IAsyncEnumerable<IDataObject>)FetchMethod.MakeGenericMethod(types[i])
                        .Invoke(this, new object[] { transaction, conditions[i], cancellationToken });
                    await foreach (var row in rows.WithCancellation(cancellationToken)) {
                        cancellationToken.ThrowIfCancellationRequested();
                        byte[] bytes = codecs[i].Export(row, MaxRowBytes);
                        await wire.WriteInt32Async(bytes.Length);
                        await wire.WriteAsync(bytes);
                        count = checked(count + 1);
                    }
                    await wire.WriteInt32Async(-1);
                    await wire.WriteInt64Async(count);
                }
            }, cancellationToken, isolationLevel);
            cancellationToken.ThrowIfCancellationRequested();
            // A failed database transaction never produces a completed archive.
            await wire.WriteAsync(EndMagic);
            await destination.WriteAsync(wire.GetHash(), cancellationToken);
        }

        /// <summary>
        /// Reads one complete backup and saves rows in batches within one transaction. All schemas are validated
        /// against caller-supplied types before any writes; corrupt/truncated backups fail the transaction.
        /// OldName aliases, removed/added columns and compatible CLR type changes are supported.
        /// Set ignoreMissingTables to skip tables deliberately removed from the current model.
        /// Does not create tables, clear existing data, or instantiate types named by the archive.
        /// </summary>
        public Task RestoreAsync(Stream source, IEnumerable<Type> tableTypes, CancellationToken cancellationToken = default,
            IsolationLevel isolationLevel = IsolationLevel.Serializable, bool ignoreMissingTables = false) {
            return RestoreWithReportAsync(source, tableTypes, cancellationToken, isolationLevel, ignoreMissingTables);
        }

        /// <summary>Restores atomically and returns mappings/counts only after successful completion.</summary>
        public async Task<BDadosBackupRestoreReport> RestoreWithReportAsync(Stream source, IEnumerable<Type> tableTypes, CancellationToken cancellationToken = default,
            IsolationLevel isolationLevel = IsolationLevel.Serializable, bool ignoreMissingTables = false) {
            return await RestoreCoreAsync(source, tableTypes, cancellationToken, isolationLevel, ignoreMissingTables,
                null, false, Options.RequireEmptyTables);
        }

        private async Task<BDadosBackupRestoreReport> RestoreCoreAsync(Stream source, IEnumerable<Type> tableTypes,
            CancellationToken cancellationToken, IsolationLevel isolationLevel, bool ignoreMissingTables,
            BDadosTransaction sharedTransaction, bool useSharedTransaction, bool requireEmpty) {
            ArgumentNullException.ThrowIfNull(source);
            if (!source.CanRead) throw new ArgumentException("The backup stream must be readable.", nameof(source));
            cancellationToken.ThrowIfCancellationRequested();
            int chunkSize = Options.RestoreChunkSize;
            if (chunkSize <= 0) throw new ArgumentOutOfRangeException(nameof(Options.RestoreChunkSize), "Restore chunk size must be positive.");
            long chunkBytes = Options.RestoreChunkBytes;
            if (chunkBytes <= 0) throw new ArgumentOutOfRangeException(nameof(Options.RestoreChunkBytes));
            int maxKeys = Options.MaxTrackedKeys;
            if (maxKeys <= 0) throw new ArgumentOutOfRangeException(nameof(Options.MaxTrackedKeys));
            long maxKeyBytes = Options.MaxTrackedKeyBytes;
            if (maxKeyBytes <= 0) throw new ArgumentOutOfRangeException(nameof(Options.MaxTrackedKeyBytes));
            var ignoredTables = new HashSet<string>(Options.IgnoredTables, StringComparer.OrdinalIgnoreCase);
            BDadosRestoreSession.RequireSupportedProvider(_accessor.QueryGenerator);
            var registered = GetTableTypes(tableTypes);
            using var wire = new Wire(source, cancellationToken);
            if (!(await wire.ReadAsync(Magic.Length)).AsSpan().SequenceEqual(Magic)) {
                throw new InvalidDataException("Invalid BDados backup signature.");
            }
            if (await wire.ReadInt32Async() != 1) {
                throw new InvalidDataException("Unsupported BDados backup version.");
            }
            int headerLength = await wire.ReadInt32Async();
            CheckLength(headerLength, MaxHeaderBytes, "header");
            byte[] header = await wire.ReadAsync(headerLength);
            byte[] headerHash = await wire.ReadAsync(32);
            if (!CryptographicOperations.FixedTimeEquals(headerHash, SHA256.HashData(header))) {
                throw new InvalidDataException("Backup header checksum mismatch.");
            }
            var schemas = ReadHeader(header);
            var mappedTypes = BDadosBackupMapping.Bind(schemas.Select(s => s.TableName).ToArray(), registered,
                t => t.Name, BDadosBackupMapping.Aliases);
            var codecs = new BDadosBackupCodec[schemas.Length];
            var bindings = new BDadosRestoreBinding[schemas.Length];
            var results = new BDadosBackupTableResult[schemas.Length];
            var saveChunks = new Func<BDadosTransaction, List<IDataObject>, Task<bool>>[schemas.Length];
            for (int i = 0; i < schemas.Length; i++) {
                var type = mappedTypes[i];
                results[i] = new BDadosBackupTableResult {
                    SourceTable = schemas[i].TableName, DestinationTable = type?.Name,
                    Skipped = ignoredTables.Contains(schemas[i].TableName) || type == null && ignoreMissingTables,
                    IgnoredColumns = Array.Empty<string>(), DefaultedColumns = Array.Empty<string>()
                };
                if (results[i].Skipped) continue;
                if (type == null) {
                    throw new InvalidDataException($"No restore model registered for table '{schemas[i].TableName}'.");
                }
                codecs[i] = new BDadosBackupCodec(type, schemas[i], forImport: true);
                bindings[i] = new BDadosRestoreBinding(type, codecs[i], _accessor.QueryGenerator);
                results[i].IgnoredColumns = codecs[i].IgnoredColumns;
                results[i].DefaultedColumns = codecs[i].DefaultedColumns;
                saveChunks[i] = SaveChunkMethod.MakeGenericMethod(type)
                    .CreateDelegate<Func<BDadosTransaction, List<IDataObject>, Task<bool>>>(this);
            }
            async ValueTask RestoreTransactionAsync(BDadosTransaction transaction) {
                if (transaction != null) {
                    transaction.RestorationMode = true;
                    transaction.Benchmarker = new Benchmarker("Restore") { Active = false };
                }
                var types = bindings.Where(b => b != null).Select(b => b.Type).ToArray();
                var session = new BDadosRestoreSession(_accessor, transaction);
                Exception restoreError = null;
                try {
                    await session.BeginAsync(types);
                    if (requireEmpty) {
                        foreach (var type in types) {
                            var query = Qb.Fmt($"SELECT COUNT(*) AS Value FROM {_accessor.QueryGenerator.QuoteBackupIdentifier(type.Name)}");
                            if ((await _accessor.QueryAsync<ValueBox<long>>(transaction, query)).Single().Value != 0) {
                                throw new InvalidDataException($"Restore requires empty destination table '{type.Name}'.");
                            }
                        }
                    }
                    for (int i = 0; i < schemas.Length; i++) {
                        if (transaction != null) transaction.RestorationBinding = bindings[i];
                        long count = 0;
                        long bufferedBytes = 0;
                        long trackedKeyBytes = 0;
                        var keys = new HashSet<object>(BackupKeyComparer.Instance);
                        var chunk = new List<IDataObject>();
                        while (true) {
                            int length = await wire.ReadInt32Async();
                            if (length == -1) break;
                            CheckLength(length, MaxRowBytes, "row");
                            byte[] row = await wire.ReadAsync(length);
                            count = checked(count + 1);
                            if (codecs[i] == null) continue; // Still read, hash and count skipped tables.
                            if (chunk.Count > 0 && bufferedBytes + length > chunkBytes) {
                                if (!await saveChunks[i](transaction, chunk)) throw new InvalidDataException($"Saving '{schemas[i].TableName}' failed.");
                                chunk.Clear();
                                bufferedBytes = 0;
                            }
                            var item = (IDataObject)codecs[i].Import(row);
                            if (keys.Count >= maxKeys) throw new InvalidDataException($"Restore identity limit exceeded for '{schemas[i].TableName}'. Increase MaxTrackedKeys to restore this table.");
                            object key = bindings[i].GetKey(item);
                            trackedKeyBytes = checked(trackedKeyBytes + 64 + (key is string text ? text.Length * 2L : key is byte[] data ? data.Length : 0));
                            if (trackedKeyBytes > maxKeyBytes) throw new InvalidDataException($"Restore identity byte limit exceeded for '{schemas[i].TableName}'. Increase MaxTrackedKeyBytes to restore this table.");
                            if (!keys.Add(key)) throw new InvalidDataException($"Duplicate primary key at row {count} of '{schemas[i].TableName}'.");
                            // Legacy saves otherwise overwrite the restored UpdatedAt timestamp.
                            if (item is ILegacyDataObject legacy) legacy.IsReceivedFromSync = true;
                            cancellationToken.ThrowIfCancellationRequested();
                            chunk.Add(item);
                            bufferedBytes += length;
                            if (chunk.Count == chunkSize) {
                                if (!await saveChunks[i](transaction, chunk)) {
                                    throw new InvalidDataException($"Saving restore batch ending at row {count} of '{schemas[i].TableName}' failed.");
                                }
                                cancellationToken.ThrowIfCancellationRequested();
                                chunk.Clear();
                                bufferedBytes = 0;
                            }
                        }
                        if (await wire.ReadInt64Async() != count) {
                            throw new InvalidDataException($"Row count mismatch in table '{schemas[i].TableName}'.");
                        }
                        if (chunk.Count > 0) {
                            cancellationToken.ThrowIfCancellationRequested();
                            if (!await saveChunks[i](transaction, chunk)) {
                                throw new InvalidDataException($"Saving restore batch ending at row {count} of '{schemas[i].TableName}' failed.");
                            }
                            cancellationToken.ThrowIfCancellationRequested();
                        }
                        results[i].RowsRead = count;
                        results[i].RowsSaved = results[i].Skipped ? 0 : count;
                    }
                    if (!(await wire.ReadAsync(EndMagic.Length)).AsSpan().SequenceEqual(EndMagic)) {
                        throw new InvalidDataException("Missing backup completion marker.");
                    }
                    byte[] expectedHash = wire.GetHash();
                    byte[] actualHash = new byte[32];
                    await source.ReadExactlyAsync(actualHash, cancellationToken);
                    if (!CryptographicOperations.FixedTimeEquals(expectedHash, actualHash)) {
                        throw new InvalidDataException("Backup checksum mismatch.");
                    }
                    await session.ValidateAsync(types);
                    foreach (var binding in bindings.Where(b => b != null)) await session.SynchronizeSequenceAsync(binding);
                } catch (Exception exception) {
                    restoreError = exception;
                    throw;
                } finally {
                    try {
                        await session.EndAsync();
                    } catch (Exception cleanupError) when (restoreError != null) {
                        throw new AggregateException("Restore failed and session settings could not be restored.", restoreError, cleanupError);
                    }
                }
            }
            if (useSharedTransaction) await RestoreTransactionAsync(sharedTransaction);
            else await _accessor.AccessAsync(RestoreTransactionAsync, cancellationToken, isolationLevel);
            return new BDadosBackupRestoreReport { Tables = Array.AsReadOnly(results) };
        }

        private Task<bool> SaveRestoreChunkAsync<T>(BDadosTransaction transaction, List<IDataObject> chunk) where T : IDataObject {
            // Preserve the concrete table type so the accessor selects its bulk save path.
            return _accessor.SaveListAsync(transaction, chunk.Cast<T>().ToList());
        }

        private async IAsyncEnumerable<IDataObject> FetchRowsAsync<T>(BDadosTransaction transaction, IQueryBuilder conditions,
            [EnumeratorCancellation] CancellationToken cancellationToken) where T : IDataObject, new() {
            await foreach (var row in _accessor.FetchAsync<T>(transaction, condicoes: conditions, skip: null, limit: null)
                .WithCancellation(cancellationToken)) {
                yield return row;
            }
        }

        private static IQueryBuilder CreateIncrementalCondition(Type type, DateTime since) {
            var members = BDadosBackupSchema.GetMembers(type);
            string created = GetTimestampMember<CreationTimeStampAttribute>(type, members, nameof(IDataObject.CreatedAt));
            string updated = GetTimestampMember<UpdateTimeStampAttribute>(type, members, nameof(IDataObject.UpdatedAt));
            return Qb.Or(Qb.Gt(created, since), Qb.Gt(updated, since));
        }

        private static string GetTimestampMember<TAttribute>(Type type, MemberInfo[] members, string conventionalName) where TAttribute : Attribute {
            var matches = members.Where(m => m.IsDefined(typeof(TAttribute), true)).ToArray();
            if (matches.Length == 0) {
                matches = members.Where(m => string.Equals(m.Name, conventionalName, StringComparison.OrdinalIgnoreCase)).ToArray();
            }
            if (matches.Length != 1) {
                throw new ArgumentException($"Incremental backup requires one persisted {conventionalName} member on '{type.FullName}', identified by its name or [{typeof(TAttribute).Name}].");
            }
            var memberType = BDadosBackupSchema.GetMemberType(matches[0]);
            if ((Nullable.GetUnderlyingType(memberType) ?? memberType) != typeof(DateTime)) {
                throw new ArgumentException($"Incremental timestamp '{type.Name}.{matches[0].Name}' must be DateTime or nullable DateTime.");
            }
            return matches[0].Name;
        }

        private static Type[] GetTableTypes(IEnumerable<Type> tableTypes) {
            ArgumentNullException.ThrowIfNull(tableTypes);
            var result = tableTypes.ToArray();
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var type in result) {
                if (type == null || !typeof(IDataObject).IsAssignableFrom(type) || !type.IsClass || type.IsAbstract
                    || type.ContainsGenericParameters || type.GetConstructor(Type.EmptyTypes) == null) {
                    throw new ArgumentException("Table types must be concrete IDataObject classes with public parameterless constructors.", nameof(tableTypes));
                }
                if (type.IsDefined(typeof(Figlotech.BDados.DataAccessAbstractions.Attributes.ViewOnlyAttribute), true)) {
                    throw new ArgumentException($"'{type.Name}' is a view, not a restorable table.", nameof(tableTypes));
                }
                if (!names.Add(type.Name)) {
                    throw new ArgumentException($"Duplicate table name '{type.Name}'.", nameof(tableTypes));
                }
            }
            return result;
        }

        private byte[] CreateHeader(BDadosBackupSchema[] schemas) {
            using var buffer = new MemoryStream();
            using var writer = new BinaryWriter(buffer, BDadosBackupCodec.Utf8, true);
            writer.Write(schemas.Length);
            foreach (var schema in schemas) {
                byte[] name = BDadosBackupCodec.Utf8.GetBytes(schema.TableName);
                byte[] bytes = schema.Serialize();
                if (buffer.Length + 8L + name.Length + bytes.Length > MaxHeaderBytes) {
                    throw new InvalidDataException("Backup header exceeds the configured byte limit.");
                }
                writer.Write(name.Length);
                writer.Write(name);
                writer.Write(bytes.Length);
                writer.Write(bytes);
            }
            return buffer.ToArray();
        }

        private static BDadosBackupSchema[] ReadHeader(byte[] header) {
            using var buffer = new MemoryStream(header, false);
            using var reader = new BinaryReader(buffer, BDadosBackupCodec.Utf8, true);
            try {
                int count = reader.ReadInt32();
                if (count < 0 || count > (header.Length - 4) / 8) {
                    throw new InvalidDataException("Invalid backup table count.");
                }
                var schemas = new BDadosBackupSchema[count];
                var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < count; i++) {
                    string name = BDadosBackupCodec.Utf8.GetString(ReadHeaderBlock(reader));
                    schemas[i] = BDadosBackupSchema.Deserialize(ReadHeaderBlock(reader));
                    if (name != schemas[i].TableName || !names.Add(name)) {
                        throw new InvalidDataException("Duplicate or inconsistent table name in backup header.");
                    }
                }
                if (buffer.Position != buffer.Length) throw new InvalidDataException("Unexpected backup header bytes.");
                return schemas;
            } catch (EndOfStreamException exception) {
                throw new InvalidDataException("Truncated backup header.", exception);
            }
        }

        private static byte[] ReadHeaderBlock(BinaryReader reader) {
            int length = reader.ReadInt32();
            if (length < 0 || length > reader.BaseStream.Length - reader.BaseStream.Position) {
                throw new InvalidDataException("Invalid backup header block length.");
            }
            return reader.ReadBytes(length);
        }

        private static void CheckLength(int length, int maximum, string description) {
            if (length < 0 || length > maximum) {
                throw new InvalidDataException($"Invalid backup {description} length ({length}); maximum is {maximum} bytes.");
            }
        }

        private sealed class BackupKeyComparer : IEqualityComparer<object> {
            internal static readonly BackupKeyComparer Instance = new BackupKeyComparer();
            public new bool Equals(object left, object right) => left is byte[] a && right is byte[] b
                ? a.AsSpan().SequenceEqual(b) : object.Equals(left, right);
            public int GetHashCode(object value) {
                if (value is not byte[] bytes) return value.GetHashCode();
                var hash = new HashCode();
                hash.AddBytes(bytes);
                return hash.ToHashCode();
            }
        }

        private sealed class Wire : IDisposable {
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The caller owns the stream; Wire disposes only its own hash state.")]
            private readonly Stream _stream;
            private readonly CancellationToken _cancellationToken;
            private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

            internal Wire(Stream stream, CancellationToken cancellationToken) {
                _stream = stream;
                _cancellationToken = cancellationToken;
            }

            internal async ValueTask WriteAsync(byte[] bytes) {
                await _stream.WriteAsync(bytes, _cancellationToken);
                _hash.AppendData(bytes);
            }

            internal async ValueTask<byte[]> ReadAsync(int length) {
                byte[] bytes = new byte[length];
                await _stream.ReadExactlyAsync(bytes, _cancellationToken);
                _hash.AppendData(bytes);
                return bytes;
            }

            internal ValueTask WriteInt32Async(int value) {
                byte[] bytes = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
                return WriteAsync(bytes);
            }

            internal ValueTask WriteInt64Async(long value) {
                byte[] bytes = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
                return WriteAsync(bytes);
            }

            internal async ValueTask<int> ReadInt32Async() => BinaryPrimitives.ReadInt32LittleEndian(await ReadAsync(4));
            internal async ValueTask<long> ReadInt64Async() => BinaryPrimitives.ReadInt64LittleEndian(await ReadAsync(8));
            internal byte[] GetHash() => _hash.GetHashAndReset();
            public void Dispose() => _hash.Dispose();
        }
    }
}
