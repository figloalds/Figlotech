using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Figlotech.DataFlow {

    public class FlowRunnerBlock : IBlock {
        private readonly FlowRunner _runner;

        public FlowRunnerBlock() : this(new FlowRunner()) { }

        public FlowRunnerBlock(FlowRunner runner) {
            _runner = runner ?? throw new ArgumentNullException(nameof(runner));
            FlowValues = new Dictionary<string, object>(StringComparer.Ordinal);
        }

        [Input("Flow to execute")]
        public Flow Flow { get; set; }

        [Input("Entry block id")]
        public string EntryBlockId { get; set; }

        [Input("Execution tick", optional: true)]
        public object Execution { get; set; }

        [Input("Additional flow values", optional: true)]
        public IDictionary<string, object> FlowValues { get; set; }

        [Output("Subflow execution result")]
        public FlowRunResult Result { get; private set; }

        public async ValueTask Execute() {
            if (Flow == null) {
                throw new InvalidOperationException("FlowRunnerBlock requires a subflow.");
            }

            if (string.IsNullOrWhiteSpace(EntryBlockId)) {
                throw new InvalidOperationException("FlowRunnerBlock requires an entry block id.");
            }

            var values = FlowValues != null
                ? new Dictionary<string, object>(FlowValues, StringComparer.Ordinal)
                : null;

            Result = await _runner.ExecuteAsync(Flow, EntryBlockId, Execution, values).ConfigureAwait(false);
        }
    }
}
