namespace Figlotech.BDados.Helpers {
    /// <summary>Settings for BDados backup operations.</summary>
    public sealed class BDadosBackupOptions {
        /// <summary>Maximum rows per restore save batch. Must be positive; read once when restore starts.</summary>
        public int RestoreChunkSize { get; set; } = 1000;
    }
}
