using System;
using System.Buffers.Binary;
using System.Data;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.Core.FileAcessAbstractions;

namespace Figlotech.BDados.Helpers {
    /// <summary>Validated checkpoint and explicit recovery information. CRC is not authentication.</summary>
    public sealed class BDadosBackupArchiveInfo {
        public long BlockCount { get; internal set; }
        public long CommittedLength { get; internal set; }
        public long IncompleteTailBytes { get; internal set; }
        public bool HasIncompleteTail => IncompleteTailBytes != 0;
        /// <summary>UTC export-start cursor of the latest validated committed block.
        /// Timestamp-based continuation is not a guarantee against backdated changes or late commits.</summary>
        public DateTime Cursor { get; internal set; }
        internal long LastTrailer { get; set; } = -1;
        internal byte[] SchemaHash { get; set; }
    }

    public sealed partial class BDadosBackup {
        private const int ArchiveHeaderSize = 64, BlockPrefixSize = 48, BlockSealSize = 24, BlockHeaderSize = 72, TrailerSize = 64;
        private static readonly byte[] ArchiveMagic = "FGBAPP02"u8.ToArray();
        private static readonly byte[] BlockMagic = "FGBLOCK2"u8.ToArray();
        private static readonly byte[] SealMagic = "FGBSEAL2"u8.ToArray();
        private static readonly byte[] TrailerMagic = "FGCOMT02"u8.ToArray();
        private static readonly byte[] CommitMagic = "FGBDONE2"u8.ToArray();
        private static readonly ConditionalWeakTable<Stream, SemaphoreSlim> ArchiveLocks = new();

        /// <summary>Creates v2 in an empty readable/writable/seekable stream. Caller owns the stream and exclusive access.
        /// FileStream commits use Flush(true); other streams provide only their own FlushAsync guarantees.</summary>
        public async Task<BDadosBackupArchiveInfo> BackupAppendableAsync(Stream destination, IEnumerable<Type> tableTypes,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable) {
            RequireArchiveStream(destination, true);
            var gate = ArchiveLocks.GetValue(destination, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken);
            try {
                if (destination.Length != 0) throw new InvalidOperationException("A new appendable archive requires an empty stream.");
                var types = GetTableTypes(tableTypes);
                byte[] schemaHash = SHA256.HashData(CreateHeader(types.Select(BDadosBackupSchema.Create).ToArray()));
                byte[] header = new byte[ArchiveHeaderSize];
                ArchiveMagic.CopyTo(header, 0);
                PutInt(header, 8, 2); PutInt(header, 12, ArchiveHeaderSize);
                schemaHash.CopyTo(header, 16); PutCrc(header, 60, BackupCrc32.Compute(header.AsSpan(0, 60)));
                destination.Position = 0;
                await destination.WriteAsync(header, cancellationToken);
                var state = new BDadosBackupArchiveInfo { CommittedLength = ArchiveHeaderSize, SchemaHash = schemaHash };
                return await WriteArchiveBlockAsync(destination, types, null, state, cancellationToken, isolationLevel);
            } finally { gate.Release(); }
        }

        /// <summary>Appends using the cursor of the latest validated committed snapshot or increment.
        /// Source timestamps must use UTC.</summary>
        public Task<BDadosBackupArchiveInfo> AppendIncrementalAsync(Stream archive, IEnumerable<Type> tableTypes,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable,
            bool recoverIncompleteTail = false) {
            return AppendIncrementalCoreAsync(archive, tableTypes, null, cancellationToken, isolationLevel, recoverIncompleteTail);
        }

        /// <summary>Appends changed rows using an explicit strict timestamp predicate. Does not track deletes or
        /// guarantee a safe watermark. Table order and schemas must equal the base snapshot. Recovery truncates only
        /// a validated uncommitted tail, and must be explicitly requested.</summary>
        public Task<BDadosBackupArchiveInfo> AppendIncrementalAsync(Stream archive, IEnumerable<Type> tableTypes, DateTime since,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable,
            bool recoverIncompleteTail = false) {
            return AppendIncrementalCoreAsync(archive, tableTypes, since, cancellationToken, isolationLevel, recoverIncompleteTail);
        }

        private async Task<BDadosBackupArchiveInfo> AppendIncrementalCoreAsync(Stream archive, IEnumerable<Type> tableTypes, DateTime? since,
            CancellationToken cancellationToken, IsolationLevel isolationLevel, bool recoverIncompleteTail) {
            RequireArchiveStream(archive, true);
            var gate = ArchiveLocks.GetValue(archive, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken);
            try {
                var types = GetTableTypes(tableTypes);
                var state = await InspectArchiveCoreAsync(archive, cancellationToken);
                DateTime cutoff = since ?? state.Cursor;
                foreach (var type in types) CreateIncrementalCondition(type, cutoff);
                if (!SHA256.HashData(CreateHeader(types.Select(BDadosBackupSchema.Create).ToArray())).AsSpan().SequenceEqual(state.SchemaHash)) {
                    throw new InvalidDataException("Append requires the same ordered tables and schemas as the base snapshot.");
                }
                RequireCompleteOrRecovery(state, recoverIncompleteTail);
                cancellationToken.ThrowIfCancellationRequested();
                if (state.HasIncompleteTail) archive.SetLength(state.CommittedLength);
                return await WriteArchiveBlockAsync(archive, types, cutoff, state, cancellationToken, isolationLevel);
            } finally { gate.Release(); }
        }

        /// <summary>Validates every committed header, link, CRC and embedded legacy payload without accessing a database.
        /// Throws for corruption or absence of a committed base; reports an incomplete final append explicitly.</summary>
        public async Task<BDadosBackupArchiveInfo> InspectAppendableAsync(Stream archive, CancellationToken cancellationToken = default) {
            RequireArchiveStream(archive, false);
            var gate = ArchiveLocks.GetValue(archive, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken);
            try { return await InspectArchiveCoreAsync(archive, cancellationToken); }
            finally { gate.Release(); }
        }

        /// <summary>Preflights the entire v2 archive before database access, then restores all checkpoints in one
        /// transaction. RequireEmptyTables applies to the base only. Caller must prevent external stream mutation.</summary>
        public async Task<BDadosBackupArchiveInfo> RestoreAppendableAsync(Stream archive, IEnumerable<Type> tableTypes,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable,
            bool ignoreMissingTables = false, bool allowIncompleteTail = false) {
            RequireArchiveStream(archive, false);
            var gate = ArchiveLocks.GetValue(archive, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken);
            try {
                var types = GetTableTypes(tableTypes);
                var state = await InspectArchiveCoreAsync(archive, cancellationToken);
                RequireCompleteOrRecovery(state, allowIncompleteTail);
                await _accessor.AccessAsync(async transaction => {
                    long offset = ArchiveHeaderSize;
                    for (long index = 0; index < state.BlockCount; index++) {
                        byte[] block = await ReadAtAsync(archive, offset, BlockHeaderSize, cancellationToken);
                        long length = GetLong(block, BlockPrefixSize);
                        using var payload = new BackupRangeStream(archive, offset + BlockHeaderSize, length);
                        await RestoreCoreAsync(payload, types, cancellationToken, isolationLevel, ignoreMissingTables,
                            transaction, true, index == 0 && Options.RequireEmptyTables);
                        offset += BlockHeaderSize + length + TrailerSize;
                    }
                }, cancellationToken, isolationLevel);
                return state;
            } finally { gate.Release(); }
        }

        /// <summary>Creates a v2 file with exclusive ownership; an existing file is never replaced.</summary>
        public async Task<BDadosBackupArchiveInfo> BackupAppendableFileAsync(string path, IEnumerable<Type> tableTypes,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable) {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
            await using var archive = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 65536, FileOptions.Asynchronous);
            return await BackupAppendableAsync(archive, tableTypes, cancellationToken, isolationLevel);
        }

        /// <summary>Creates a v2 file through the filesystem without replacing an existing file.
        /// The provider must return a readable/writable/seekable stream and honor CreateNew.
        /// Sharing and durability depend on the provider; caller must ensure exclusive access.</summary>
        public async Task<BDadosBackupArchiveInfo> BackupAppendableFileAsync(IFileSystem fs, string relativePath, IEnumerable<Type> tableTypes,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable) {
            ArgumentNullException.ThrowIfNull(fs);
            ArgumentException.ThrowIfNullOrWhiteSpace(relativePath);
            cancellationToken.ThrowIfCancellationRequested();
            await using var archive = await fs.OpenAsync(relativePath, FileMode.CreateNew, FileAccess.ReadWrite);
            return await BackupAppendableAsync(archive, tableTypes, cancellationToken, isolationLevel);
        }

        /// <summary>Exclusively opens the existing v2 file for validation and durable append.</summary>
        public async Task<BDadosBackupArchiveInfo> AppendIncrementalFileAsync(string path, IEnumerable<Type> tableTypes,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable, bool recoverIncompleteTail = false) {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
            await using var archive = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 65536, FileOptions.Asynchronous);
            return await AppendIncrementalAsync(archive, tableTypes, cancellationToken, isolationLevel, recoverIncompleteTail);
        }

        /// <summary>Exclusively opens the existing v2 file for validation and durable append with an explicit cutoff.</summary>
        public async Task<BDadosBackupArchiveInfo> AppendIncrementalFileAsync(string path, IEnumerable<Type> tableTypes, DateTime since,
            CancellationToken cancellationToken = default, IsolationLevel isolationLevel = IsolationLevel.Serializable, bool recoverIncompleteTail = false) {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
            await using var archive = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 65536, FileOptions.Asynchronous);
            return await AppendIncrementalAsync(archive, tableTypes, since, cancellationToken, isolationLevel, recoverIncompleteTail);
        }

        /// <summary>Restores legacy v1 or appendable v2 files. Exclusive read ownership prevents concurrent writers.</summary>
        public async Task RestoreFileAsync(string path, IEnumerable<Type> tableTypes, CancellationToken cancellationToken = default,
            IsolationLevel isolationLevel = IsolationLevel.Serializable, bool ignoreMissingTables = false, bool allowIncompleteTail = false) {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
            await using var archive = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None, 65536, FileOptions.Asynchronous);
            byte[] magic = await ReadAtAsync(archive, 0, 8, cancellationToken);
            archive.Position = 0;
            if (magic.AsSpan().SequenceEqual(ArchiveMagic)) await RestoreAppendableAsync(archive, tableTypes, cancellationToken, isolationLevel, ignoreMissingTables, allowIncompleteTail);
            else await RestoreAsync(archive, tableTypes, cancellationToken, isolationLevel, ignoreMissingTables);
        }

        private async Task<BDadosBackupArchiveInfo> WriteArchiveBlockAsync(Stream archive, Type[] types, DateTime? since,
            BDadosBackupArchiveInfo previous, CancellationToken token, IsolationLevel isolation) {
            long start = previous.CommittedLength;
            // Capture before any database reads, never after export: concurrent changes may need replaying.
            DateTime cursor = DateTime.UtcNow;
            byte[] prefix = new byte[BlockPrefixSize];
            BlockMagic.CopyTo(prefix, 0); PutInt(prefix, 8, since.HasValue ? 2 : 1); PutInt(prefix, 12, 2);
            PutLong(prefix, 16, previous.BlockCount); PutLong(prefix, 24, previous.LastTrailer);
            PutLong(prefix, 32, cursor.Ticks); PutInt(prefix, 40, (int)cursor.Kind);
            PutCrc(prefix, 44, BackupCrc32.Compute(prefix.AsSpan(0, 44)));
            archive.Position = start;
            await archive.WriteAsync(prefix, token);
            await archive.WriteAsync(new byte[BlockSealSize], token);
            uint crc = await BackupChannelStream.CopyWithCrcAsync(archive,
                (output, ct) => BackupCoreAsync(output, types, since, ct, isolation), token);
            long end = archive.Position;
            long length = checked(end - start - BlockHeaderSize);
            byte[] seal = new byte[BlockSealSize];
            PutLong(seal, 0, length); PutCrc(seal, 8, crc); PutCrc(seal, 12, BackupCrc32.Compute(seal.AsSpan(0, 12))); SealMagic.CopyTo(seal, 16);
            // Backfill only the new, uncommitted seal; all older trailers remain untouched.
            archive.Position = start + BlockPrefixSize;
            await archive.WriteAsync(seal, token);
            archive.Position = end;
            await FlushArchiveAsync(archive, token);
            token.ThrowIfCancellationRequested();
            byte[] trailer = new byte[TrailerSize];
            TrailerMagic.CopyTo(trailer, 0); PutLong(trailer, 8, start); PutLong(trailer, 16, previous.LastTrailer);
            PutLong(trailer, 24, previous.BlockCount); PutLong(trailer, 32, length); PutCrc(trailer, 40, crc);
            PutCrc(trailer, 44, GetCrc(prefix, 44)); PutCrc(trailer, 48, GetCrc(seal, 12));
            PutCrc(trailer, 52, BackupCrc32.Compute(trailer.AsSpan(0, 52))); CommitMagic.CopyTo(trailer, 56);
            await archive.WriteAsync(trailer, token);
            await FlushArchiveAsync(archive, token);
            return new BDadosBackupArchiveInfo { BlockCount = previous.BlockCount + 1, CommittedLength = archive.Position,
                LastTrailer = end, SchemaHash = previous.SchemaHash, Cursor = cursor };
        }

        private async Task<BDadosBackupArchiveInfo> InspectArchiveCoreAsync(Stream archive, CancellationToken token) {
            long fileLength = archive.Length;
            if (fileLength < ArchiveHeaderSize) throw new InvalidDataException("Missing complete v2 archive header.");
            byte[] header = await ReadAtAsync(archive, 0, ArchiveHeaderSize, token);
            if (!header.AsSpan(0, 8).SequenceEqual(ArchiveMagic) || GetInt(header, 8) != 2 || GetInt(header, 12) != ArchiveHeaderSize
                || !header.AsSpan(48, 12).SequenceEqual(new byte[12]) || GetCrc(header, 60) != BackupCrc32.Compute(header.AsSpan(0, 60))) {
                throw new InvalidDataException("Invalid v2 archive header or CRC.");
            }
            var state = new BDadosBackupArchiveInfo { CommittedLength = ArchiveHeaderSize, SchemaHash = header[16..48] };
            while (state.CommittedLength < fileLength) {
                token.ThrowIfCancellationRequested();
                long start = state.CommittedLength;
                if (fileLength - start < BlockHeaderSize) break;
                byte[] block = await ReadAtAsync(archive, start, BlockHeaderSize, token);
                int kind = GetInt(block, 8);
                if (!block.AsSpan(0, 8).SequenceEqual(BlockMagic) || GetInt(block, 12) != 2
                    || kind != (state.BlockCount == 0 ? 1 : 2) || GetLong(block, 16) != state.BlockCount
                    || GetLong(block, 24) != state.LastTrailer || GetCrc(block, 44) != BackupCrc32.Compute(block.AsSpan(0, 44))
                    || GetLong(block, 32) < 0 || GetLong(block, 32) > DateTime.MaxValue.Ticks
                    || GetInt(block, 40) != (int)DateTimeKind.Utc) {
                    throw new InvalidDataException("Corrupt block prefix or checkpoint linkage.");
                }
                bool sealedBlock = block.AsSpan(64, 8).SequenceEqual(SealMagic)
                    && GetCrc(block, 60) == BackupCrc32.Compute(block.AsSpan(48, 12));
                if (!sealedBlock) {
                    // A crashed backfill can leave arbitrary seal bytes. An intact matching trailer proves
                    // that this was committed, so it must never be silently dropped as an incomplete append.
                    if (await HasCommitEvidenceAsync(archive, start, fileLength, state, token)) throw new InvalidDataException("Committed block seal is corrupt.");
                    break;
                }
                long length = GetLong(block, 48);
                if (length < 0 || length > long.MaxValue - start - BlockHeaderSize - TrailerSize) throw new InvalidDataException("Invalid block payload length.");
                long trailerOffset = start + BlockHeaderSize + length;
                if (fileLength - trailerOffset < TrailerSize) {
                    if (await HasCommitEvidenceAsync(archive, start, fileLength, state, token)) throw new InvalidDataException("Committed block length is corrupt.");
                    break;
                }
                byte[] trailer = await ReadAtAsync(archive, trailerOffset, TrailerSize, token);
                if (!ValidTrailer(trailer) || GetLong(trailer, 8) != start || GetLong(trailer, 16) != state.LastTrailer
                    || GetLong(trailer, 24) != state.BlockCount || GetLong(trailer, 32) != length
                    || GetCrc(trailer, 40) != GetCrc(block, 56) || GetCrc(trailer, 44) != GetCrc(block, 44) || GetCrc(trailer, 48) != GetCrc(block, 60)) {
                    throw new InvalidDataException("Corrupt committed trailer or block metadata.");
                }
                using (var payload = new BackupRangeStream(archive, start + BlockHeaderSize, length)) {
                    var crc = new BackupCrc32();
                    byte[] buffer = new byte[65536];
                    int read;
                    while ((read = await payload.ReadAsync(buffer.AsMemory(), token)) != 0) crc.Append(buffer.AsSpan(0, read));
                    if (crc.Value != GetCrc(block, 56)) throw new InvalidDataException("Committed payload CRC mismatch.");
                }
                using (var payload = new BackupRangeStream(archive, start + BlockHeaderSize, length)) await ValidateArchivePayloadAsync(payload, state.SchemaHash, token);
                state.BlockCount++;
                state.LastTrailer = trailerOffset;
                state.CommittedLength = trailerOffset + TrailerSize;
                state.Cursor = new DateTime(GetLong(block, 32), DateTimeKind.Utc);
            }
            if (state.BlockCount == 0) throw new InvalidDataException("Archive has no committed base snapshot.");
            state.IncompleteTailBytes = fileLength - state.CommittedLength;
            return state;
        }

        private async Task ValidateArchivePayloadAsync(Stream payload, byte[] schemaHash, CancellationToken token) {
            using var wire = new Wire(payload, token);
            if (!(await wire.ReadAsync(8)).AsSpan().SequenceEqual(Magic) || await wire.ReadInt32Async() != 1) throw new InvalidDataException("Invalid embedded backup version.");
            int length = await wire.ReadInt32Async(); CheckLength(length, MaxHeaderBytes, "header");
            byte[] header = await wire.ReadAsync(length);
            if (!SHA256.HashData(header).AsSpan().SequenceEqual(schemaHash) || !(await wire.ReadAsync(32)).AsSpan().SequenceEqual(schemaHash)) throw new InvalidDataException("Embedded schema differs from archive schema.");
            foreach (var schema in ReadHeader(header)) {
                long count = 0;
                while (true) {
                    int rowLength = await wire.ReadInt32Async();
                    if (rowLength == -1) break;
                    CheckLength(rowLength, MaxRowBytes, "row");
                    await wire.ReadAsync(rowLength);
                    count = checked(count + 1);
                }
                if (await wire.ReadInt64Async() != count) throw new InvalidDataException("Embedded row count mismatch.");
            }
            if (!(await wire.ReadAsync(8)).AsSpan().SequenceEqual(EndMagic)) throw new InvalidDataException("Invalid embedded completion marker.");
            byte[] hash = new byte[32]; await payload.ReadExactlyAsync(hash, token);
            if (!CryptographicOperations.FixedTimeEquals(hash, wire.GetHash()) || payload.Position != payload.Length) throw new InvalidDataException("Invalid embedded checksum or payload length.");
        }

        private static async Task<bool> HasCommitEvidenceAsync(Stream archive, long start, long fileLength, BDadosBackupArchiveInfo state, CancellationToken token) {
            // Fixed-sized overlapping windows: payload size does not grow recovery memory.
            byte[] buffer = new byte[65536 + TrailerSize - 1];
            long position = start + BlockHeaderSize;
            while (fileLength - position >= TrailerSize) {
                int count = (int)Math.Min(buffer.Length, fileLength - position);
                archive.Position = position; await archive.ReadExactlyAsync(buffer.AsMemory(0, count), token);
                for (int i = 0; i <= count - TrailerSize; i++) {
                    if (!buffer.AsSpan(i, 8).SequenceEqual(TrailerMagic)) continue;
                    byte[] candidate = buffer.AsSpan(i, TrailerSize).ToArray();
                    if (ValidTrailer(candidate) && GetLong(candidate, 8) == start && GetLong(candidate, 16) == state.LastTrailer
                        && GetLong(candidate, 24) == state.BlockCount && GetLong(candidate, 32) == position + i - start - BlockHeaderSize) return true;
                    // A later checkpoint proves that this is damaged history, even if this block's own
                    // seal and trailer are both damaged. Never discard later committed deltas as tail.
                    if (IsLaterCommitEvidence(candidate, position + i, start, state)) return true;
                }
                position += count - TrailerSize + 1;
            }
            return false;
        }

        private static bool IsLaterCommitEvidence(byte[] trailer, long trailerOffset, long suspectStart, BDadosBackupArchiveInfo state) {
            if (!ValidTrailer(trailer)) return false;
            long blockStart = GetLong(trailer, 8), length = GetLong(trailer, 32);
            if (blockStart <= suspectStart || blockStart > trailerOffset - BlockHeaderSize || length < 0
                || length != trailerOffset - blockStart - BlockHeaderSize || GetLong(trailer, 24) <= state.BlockCount
                || GetLong(trailer, 16) != blockStart - TrailerSize || GetLong(trailer, 16) < suspectStart + BlockHeaderSize) return false;
            // This is evidence to REFUSE recovery, not permission to restore this block. Requiring
            // its prefix/seal/payload to remain intact would silently discard committed corruption.
            return true;
        }

        private static bool ValidTrailer(byte[] trailer) => trailer.AsSpan(0, 8).SequenceEqual(TrailerMagic)
            && trailer.AsSpan(56, 8).SequenceEqual(CommitMagic) && GetCrc(trailer, 52) == BackupCrc32.Compute(trailer.AsSpan(0, 52));
        private static async Task FlushArchiveAsync(Stream archive, CancellationToken token) {
            await archive.FlushAsync(token);
            if (archive is FileStream file) file.Flush(flushToDisk: true);
        }
        private static void RequireCompleteOrRecovery(BDadosBackupArchiveInfo state, bool allow) {
            if (state.HasIncompleteTail && !allow) throw new InvalidDataException($"Archive has {state.IncompleteTailBytes} uncommitted tail bytes. Explicit recovery is required.");
        }
        private static void RequireArchiveStream(Stream stream, bool writable) {
            ArgumentNullException.ThrowIfNull(stream);
            if (!stream.CanRead || !stream.CanSeek || (writable && !stream.CanWrite)) throw new ArgumentException("Archive requires a readable, seekable stream and writing when appending.", nameof(stream));
        }
        private static async Task<byte[]> ReadAtAsync(Stream stream, long offset, int count, CancellationToken token) {
            stream.Position = offset; byte[] bytes = new byte[count]; await stream.ReadExactlyAsync(bytes, token); return bytes;
        }
        private static long GetLong(byte[] bytes, int offset) => BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(offset));
        private static int GetInt(byte[] bytes, int offset) => BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset));
        private static uint GetCrc(byte[] bytes, int offset) => BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset));
        private static void PutLong(byte[] bytes, int offset, long value) => BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(offset), value);
        private static void PutInt(byte[] bytes, int offset, int value) => BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), value);
        private static void PutCrc(byte[] bytes, int offset, uint value) => BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset), value);
    }
}
