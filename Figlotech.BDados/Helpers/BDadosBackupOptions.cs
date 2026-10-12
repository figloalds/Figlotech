using System;
using System.Collections.Generic;

namespace Figlotech.BDados.Helpers {
    /// <summary>Settings for BDados backup operations.</summary>
    public sealed class BDadosBackupOptions {
        /// <summary>Maximum rows per restore save batch. Must be positive; read once when restore starts.</summary>
        public int RestoreChunkSize { get; set; } = 1000;
        /// <summary>Encoded bytes per batch; one larger row is permitted up to MaxRowBytes.</summary>
        public long RestoreChunkBytes { get; set; } = 16 * 1024 * 1024;
        /// <summary>Bound for the per-table identity index. Exceeding it aborts rather than losing duplicate detection.</summary>
        public int MaxTrackedKeys { get; set; } = 1_000_000;
        /// <summary>Approximate per-table identity-index byte budget, including key payloads and entry overhead.</summary>
        public long MaxTrackedKeyBytes { get; set; } = 64 * 1024 * 1024;
        /// <summary>Require every mapped destination table to be empty. Otherwise restore merges by primary key.</summary>
        public bool RequireEmptyTables { get; set; }
        /// <summary>Exact archive table names intentionally excluded. Snapshotted when restore starts.</summary>
        public ISet<string> IgnoredTables { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    }
}
