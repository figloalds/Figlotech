using System;
using System.Collections.Generic;
using System.IO;

namespace Figlotech.Core.Helpers {
    public class BatchStreamProcessor : IStreamProcessor {
        List<IStreamProcessor> processors = new List<IStreamProcessor>();
        public BatchStreamProcessor() { }

        public bool Enable { get; set; } = true;

        public void Add(IStreamProcessor processor) {
            processors.Add(processor);
        }

        public void Process(Stream input, Action<Stream> act, int n) {
            if (!Enable) {
                using (input) {
                    act?.Invoke(input);
                }
            }
            if (processors.Count < 1 || n == processors.Count)
                act?.Invoke(input);
            else {
                processors[n]?.Process(input, (output) => {
                    Process(output, act, n + 1);
                });
            }
        }

        public void Process(Stream input, Action<Stream> act) {
            Process(input, act, 0);
        }
    }
}
