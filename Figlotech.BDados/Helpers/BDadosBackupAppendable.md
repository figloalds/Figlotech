# Same-file appendable BDados backups (container v2)

`BDadosBackup` keeps its v1 `BackupAsync`, `BackupIncrementalAsync`,
`BackupFileAsync`, `RestoreAsync` and `RestoreWithReportAsync` behavior, including
forward-only streams. The appendable APIs below are separate and require readable,
seekable streams (also writable for creation/append). They never close caller streams.

```csharp
var backup = new BDadosBackup(accessor);
Type[] tables = { typeof(Customer), typeof(Order) };
await backup.BackupAppendableFileAsync(path, tables, cancellationToken);
// Resume from the cursor in the latest committed snapshot or increment:
await backup.AppendIncrementalFileAsync(path, tables, cancellationToken);
await backup.RestoreFileAsync(path, tables, cancellationToken);
```

To create an archive through an `IFileSystem`, use
`await backup.BackupAppendableFileAsync(fs, relativePath, tables, cancellationToken)`.
This overload opens with `FileMode.CreateNew` and `FileAccess.ReadWrite` and disposes
the returned stream. The provider must honor create-new semantics and return a
readable, writable, seekable stream. Sharing depends on the provider; callers must
ensure exclusive access throughout creation.

`RestoreFileAsync` detects v1 versus v2. `RestoreAppendableAsync` restores v2
streams and returns `BDadosBackupArchiveInfo`. `InspectAppendableAsync` validates
v2 without accessing the database and returns `BlockCount`, `CommittedLength`,
`IncompleteTailBytes`, `HasIncompleteTail` and `Cursor`. Stream creation is
`BackupAppendableAsync`; stream append is `AppendIncrementalAsync`.

Every new snapshot and increment stores a UTC `Cursor` captured before exporting
its rows. The overloads without `since` validate the archive and use the latest
committed block's cursor as the next query cutoff, under the same exclusive lock
used to append. Empty increments also save a cursor. Incomplete tails never advance
it; explicit recovery resumes from the last committed block. Inspection and restore
return that same cursor. A corrupt committed block is an error, not permission to
fall back to an older cursor.

The explicit `since` overload remains available for conservative overlapping
cutoffs and source databases that do not use UTC timestamps:

```csharp
await backup.AppendIncrementalFileAsync(path, tables, since, cancellationToken);
```

Creation requires an empty stream; file creation uses `CreateNew` and never replaces
an existing file. Failed initial creation may leave an unusable partial file; no
base snapshot is reported committed until its trailer is complete. Start a new file
after preserving/removing that failed initial artifact as appropriate.

## Incremental semantics

The base is a full export of explicitly selected tables. Each delta is the existing
`CreatedAt > since OR UpdatedAt > since` query, with timestamp attributes honored.
Restore merges by primary key and preserves timestamps. **Deletes are not recorded**;
deleted source rows remain in the restored result. This is not CDC or a guaranteed
point-in-time database replica. Backdated changes, unchanged/null timestamps,
timestamp precision collisions and transactions committing after an application
cutoff can be missed. The stored cursor is the export-start time, not the export-end
time or a database commit watermark; exports can also include rows newer than it.
Automatic continuation requires source timestamps in UTC and clocks consistent with
the backup host. It does not claim a gap-free interval. Use explicit conservative
overlapping cutoffs and periodic full snapshots appropriate to the writers when
needed. Overlap is safe for ordinary upserts; it cannot recover missing deletes.
Explicit query cutoffs are used without timezone conversion.

The ordered table list and exact serialized schemas must match the base for every
append; a model/schema change requires a new base file. Existing restore mappings
still support importing that fixed archived schema into a changed destination model.

## Binary format

All numbers are little-endian. Offsets, lengths, sequence and timestamp ticks are signed
64-bit integers; kinds/versions are signed 32-bit; CRCs are unsigned 32-bit.
CRC-32/ISO-HDLC uses reflected polynomial `0xEDB88320`, initial value and final XOR
`0xFFFFFFFF` (check vector `123456789` = `CBF43926`). SHA-256 is retained inside v1
payloads. Neither CRC nor unkeyed SHA provides authenticity against deliberate editing.

File header, 64 bytes:

| Offset | Field |
| --- | --- |
| 0 | 8 ASCII bytes `FGBAPP02` |
| 8 | Version = 2 |
| 12 | Header size = 64 |
| 16 | 32-byte SHA-256 of the embedded v1 schema header |
| 48 | 12 reserved zero bytes |
| 60 | CRC of header bytes 0..59 |

Blocks are contiguous immediately after the file header and preceding trailers.
There is exactly one full block (sequence 0); all subsequent blocks are deltas.
The immutable prefix occupies 48 bytes:

| Offset | Field |
| --- | --- |
| 0 | 8 ASCII bytes `FGBLOCK2` |
| 8 | Kind: 1 full, 2 incremental |
| 12 | Version = 2 |
| 16 | Sequence, starting at 0 |
| 24 | Absolute previous trailer offset; -1 for the base |
| 32 | Export-start cursor ticks, for both snapshots and increments |
| 40 | Cursor DateTimeKind = UTC (1) |
| 44 | CRC of prefix bytes 0..43 |

The 24-byte seal at block offset 48 is initially all zero. After streaming the
payload, the writer seeks back **only within this new uncommitted block**:

| Offset in block | Field |
| --- | --- |
| 48 | Payload byte length |
| 56 | CRC over every payload byte |
| 60 | CRC of seal bytes at offsets 48..59 |
| 64 | 8 ASCII bytes `FGBSEAL2` |

Payload begins at block offset 72 and is one complete v1 archive, including schema
header, row frames, table row counts, `FGBEND01` and SHA-256. Each delta repeats its
schema so existing codecs and checks remain applicable. The base contains all data.

The 64-byte commit trailer immediately follows the payload:

| Offset in trailer | Field |
| --- | --- |
| 0 | 8 ASCII bytes `FGCOMT02` |
| 8 | Absolute current block offset |
| 16 | Absolute previous trailer offset |
| 24 | Sequence |
| 32 | Payload length |
| 40 | Payload CRC |
| 44 | Immutable prefix CRC |
| 48 | Seal CRC |
| 52 | CRC of trailer bytes 0..51 |
| 56 | 8 ASCII bytes `FGBDONE2` |

## Commit protocol and ownership

1. Acquire exclusive ownership; validate the entire existing chain and payloads.
2. If explicitly requested, truncate only the identified uncommitted final tail.
3. Append immutable prefix and zero seal; serialize through a bounded channel.
4. SplitStream forks concurrently copy bytes to the file and compute CRC. All
   producer/copy/hash tasks share cancellation and are joined even on failure.
5. Seek to the new seal, write length and CRC, then seek to the payload end.
6. `FlushAsync`, then `FileStream.Flush(flushToDisk: true)`; only afterward write the trailer.
7. Flush the trailer with the same ordering. Return success only after this completes.

Older committed bytes, including the preceding trailer, are never rewritten.
Cancellation before trailer publication leaves a recoverable tail. An error or
cancellation during trailer write/flush has an **uncertain outcome**: inspect the
file before retrying. A complete valid trailer may already represent a committed
append even if the call threw. Replaying a delta normally merges the same keys but
creates another checkpoint; callers must not infer "nothing happened" from an error.

Local-path file APIs hold `FileShare.None` from validation through final flush/restore. Calls
through these APIs on the same Stream object serialize via a per-stream gate;
different wrappers/handles require caller coordination. The caller must prohibit
external writes, seeks, disposal and concurrent use of the accessor throughout a
stream operation. Do not replace files behind open handles. File sharing/advisory
locks and flush semantics depend on the filesystem, operating system, device and
remote mount. Guarantees assume successful durable flushes are honored and previous
stable bytes are not subsequently damaged. The library cannot guarantee directory
entry durability after creation, faulty controller behavior, network filesystem
durability, or arbitrary sector damage to previously committed bytes. For custom
Stream implementations, durability is only what their `FlushAsync` provides.

## Validation and recovery

Inspection walks the contiguous chain with bounded memory, verifies file/prefix/seal/
trailer fields and CRCs, rejects overflow and negative lengths, checks backwards
linkage and sequence, verifies payload CRC and then v1 schema hash, framing, row
counts and SHA-256. No payload is restored before this preflight completes. Restore
then applies every block in **one database transaction**; `RequireEmptyTables`
applies only to the base. Destination mapping/conversion and save failures roll
back that transaction. Existing provider limitations still apply.

A physically short final header/payload/trailer is an incomplete append. A torn seal
backfill is also incomplete unless a fully validated trailer at the corresponding
physical offset proves that block committed; in that case it is corruption. Trailer
search uses bounded overlapping buffers and validates linkage, sequence, length,
CRC and both magics; merely finding magic text inside user payload is not a commit.
A complete invalid prefix or trailer, or any invalid committed payload, is corruption
and throws. It never silently selects an older good checkpoint instead.
When a damaged seal requires a commit-evidence scan, a later structurally valid
trailer also proves that the suspect middle block cannot be an uncommitted tail.
The scan checks both trailer magics and its CRC, physical offsets/length, later
sequence and contiguous predecessor position. That evidence is grounds to refuse
recovery even when the later prefix, seal or payload is damaged. It never permits
restoring an unvalidated block. Requiring intact later data before recognizing its
commit record would silently discard committed corruption. A fabricated CRC-valid
trailer with matching absolute geometry can cause conservative refusal; this format
does not authenticate hostile input.

There is no durability barrier between the initial prefix and its payload. Before
the first successful durable flush, a device may persist payload bytes but tear the
prefix. If the resulting complete-sized prefix fails validation, recovery refuses
the ambiguous suffix, even with explicit recovery enabled. Earlier committed bytes
are preserved, but automatic tail recovery is not guaranteed for arbitrary reordered
sector writes. Byte-prefix truncation/torn-backfill tests model ordered logical
writes; the separate torn-prefix test verifies this conservative refusal.

An incomplete tail is reported explicitly. Restore and append reject it by default.
To recover, inspect first and deliberately pass `allowIncompleteTail: true` to
restore or `recoverIncompleteTail: true` to append. Restore leaves the file intact;
append removes only those uncommitted bytes before starting a replacement append.
No committed base means failure, never an empty successful restore.

There is an unavoidable distinction limit: externally truncating an already committed
final append can look exactly like an interrupted append. Losing an entire final
checkpoint at its prior boundary is indistinguishable from a shorter valid history.
Multiple coordinated corruptions that remove both a seal and all commit evidence
can also look incomplete. Defaults require explicit recovery approval through the API;
applications needing rollback detection must retain an independent trusted checkpoint
or authenticated manifest. CRC is for accidental corruption, not hostile tampering.

Memory is bounded by configured header/row and restore-batch/key limits, plus under
2 MiB of producer/fan-out/copy/hash buffers; no whole archive or block list is retained.
Inspection reads payloads twice (CRC and embedded v1 validation); append revalidates
the existing history, so append cost grows with total archive size. Restore performs
another streaming read in its transaction. This favors integrity over constant-time
append. SplitStream now discards both consumed data and its associated metadata,
uses cancellable waits, and wakes blocked forks on disposal or source failure.
