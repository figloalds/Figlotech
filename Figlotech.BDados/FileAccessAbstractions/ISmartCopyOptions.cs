using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.FileAcessAbstractions
{
    /// <summary>
    /// This interface is for IoC injection cases.
    /// </summary>
    public interface ISmartCopyOptions {
        /// <summary>
        /// If set to true (default), the comparator will calculate the file hash to determine
        /// if it has or not changed instead of date and length.
        /// Disable if using a over-internet file accessor like the BlobFileAccessor.
        /// </summary>
        bool UseHash { get; set; }
        /// <summary>
        /// If set to true (default), the comparator will calculate the file hash to determine
        /// if it has or not changed instead of date and length.
        /// Disable if using a over-internet file accessor like the BlobFileAccessor.
        /// </summary>
        bool UseHashList { get; set; }
        /// <summary>
        /// Consider older files as "not changed" and ignores them
        /// </summary>
        bool IgnoreOlder { get; set; }
        /// <summary>
        /// If true (default) the algorithm will work on all files in the tree,
        /// if set to false only the top level will be scanned and worked on.
        /// </summary>
        bool Recursive { get; set; }
        /// <summary>
        /// if set to true (default) the algorithm will spawn multiple threads to copy multiple files
        /// at a time.
        /// </summary>
        bool Multithreaded { get; set; }
        /// <summary>
        /// Number of simultaneous copy operations (default is Environment.ProcessorCount)
        /// </summary>
        int NumWorkers { get; set; }
        /// <summary>
        /// Number of simultaneous copy operations (default is Environment.ProcessorCount)
        /// </summary>
        bool AllowDelete { get; set; }
        /// <summary>
        /// Number of bytes to use as memory buffer when copying
        /// This total buffer will be shared between the copy workers.
        /// Default is 4MB
        /// </summary>
        long BufferSize { get; set; }
    }
}
