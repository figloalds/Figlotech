using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {
    public class SmartCopyOptions
    {
        /// <summary>
        /// If set to true (default), the comparator will calculate the file hash to determine
        /// if it has or not changed instead of date and length.
        /// Disable if using a over-internet file accessor like the BlobFileAccessor.
        /// </summary>
        public bool UseHash { get; set; } = true;
        /// <summary>
        /// If set to true (default), the comparator will calculate the file hash to determine
        /// if it has or not changed instead of date and length.
        /// Disable if using a over-internet file accessor like the BlobFileAccessor.
        /// </summary>
        public bool UseHashList { get; set; } = false;
        /// <summary>
        /// Consider older files as "not changed" and ignores them
        /// </summary>
        public bool IgnoreOlder { get; set; } = true;
        /// <summary>
        /// If true (default) the algorithm will work on all files in the tree,
        /// if set to false only the top level will be scanned and worked on.
        /// </summary>
        public bool Recursive { get; set; } = true;
        /// <summary>
        /// if set to true (default) the algorithm will spawn multiple threads to copy multiple files
        /// at a time.
        /// </summary>
        public bool Multithreaded { get; set; } = true;
        /// <summary>
        /// Number of simultaneous copy operations (default is Environment.ProcessorCount)
        /// </summary>
        public int NumWorkers { get; set; } = Environment.ProcessorCount;
        /// <summary>
        /// Number of simultaneous copy operations (default is Environment.ProcessorCount)
        /// </summary>
        public bool AllowDelete { get; set; } = false;
        /// <summary>
        /// Number of bytes to use as memory buffer when copying
        /// This total buffer will be shared between the copy workers.
        /// Default is 4MB
        /// </summary>
        public long BufferSize { get; set; } = 4 * 1024 * 1024;
        /// <summary>
        /// Number of bytes to use as memory buffer when copying
        /// This total buffer will be shared between the copy workers.
        /// Default is 4MB
        /// </summary>
        public bool UseGZip { get; set; } = false;
    }
}
