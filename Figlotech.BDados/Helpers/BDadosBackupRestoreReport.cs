using System.Collections.Generic;

namespace Figlotech.BDados.Helpers {
    public sealed class BDadosBackupRestoreReport {
        public IReadOnlyList<BDadosBackupTableResult> Tables { get; internal set; }
    }

    public sealed class BDadosBackupTableResult {
        public string SourceTable { get; internal set; }
        public string DestinationTable { get; internal set; }
        public bool Skipped { get; internal set; }
        public long RowsRead { get; internal set; }
        public long RowsSaved { get; internal set; }
        public IReadOnlyList<string> IgnoredColumns { get; internal set; }
        public IReadOnlyList<string> DefaultedColumns { get; internal set; }
    }
}
