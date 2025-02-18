using System;
using System.Collections.Generic;

namespace Figlotech.Core {

    public static partial class FiTechCoreExtensions {
        public sealed class FlowYield<T> {
            IParallelFlowStepIn<T> root { get; set; }
            ParallelFlowOutEnumerator<T> Enumerator { get; set; }
            public FlowYield(IParallelFlowStepIn<T> root, ParallelFlowOutEnumerator<T> enumerator) {
                this.root = root;
                this.Enumerator = enumerator;
            }
            public void Return(T o) {
                root.Put(o);
                Enumerator.Publish(o);
            }
            public void ReturnRange(IEnumerable<T> list) {
                list.ForEach(x => Return(x));
            }
        }
    }
}