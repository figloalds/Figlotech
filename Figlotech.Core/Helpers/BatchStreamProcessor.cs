using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public sealed class BatchStreamProcessor : IStreamProcessor {
        List<IStreamProcessor> processors = new List<IStreamProcessor>();
        public BatchStreamProcessor() { }

        public bool Enable { get; set; } = true;

        public void Add(IStreamProcessor processor) {
            processors.Add(processor);
        }

        public async Task Process(Stream input, Func<Stream, Task> act, int n) {
            if (!Enable) {
                using (input) {
                    await act?.Invoke(input);
                }
            }
            if (processors.Count < 1 || n == processors.Count)
                await act?.Invoke(input);
            else {
                await processors[n]?.Process(input, async (output) => {
                    await Process(output, act, n + 1);
                });
            }
        }

        public async Task Process(Stream input, Func<Stream, Task> act) {
            await Process(input, act, 0);
        }
    }
}
