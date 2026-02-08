using System;
using System.Collections.Generic;
using System.Linq;

namespace Figlotech.DataFlow {

    public enum FlowInputSourceKind {
        Connection,
        Constant,
        FlowValue,
    }

    public sealed class FlowInputBinding {
        public string TargetBlockId { get; set; }
        public string TargetInputName { get; set; }
        public FlowInputSourceKind SourceKind { get; set; }
        public string SourceBlockId { get; set; }
        public string SourceOutputName { get; set; }
        public object ConstantValue { get; set; }
        public string FlowValueName { get; set; }

        public FlowInputBinding() { }

        private FlowInputBinding(string targetBlockId, string targetInputName, FlowInputSourceKind sourceKind, string sourceBlockId, string sourceOutputName, object constantValue, string flowValueName) {
            TargetBlockId = targetBlockId;
            TargetInputName = targetInputName;
            SourceKind = sourceKind;
            SourceBlockId = sourceBlockId;
            SourceOutputName = sourceOutputName;
            ConstantValue = constantValue;
            FlowValueName = flowValueName;
        }

        public static FlowInputBinding FromConnection(string sourceBlockId, string sourceOutputName, string targetBlockId, string targetInputName) {
            return new FlowInputBinding(targetBlockId, targetInputName, FlowInputSourceKind.Connection, sourceBlockId, sourceOutputName, null, null);
        }

        public static FlowInputBinding FromConstant(string targetBlockId, string targetInputName, object value) {
            return new FlowInputBinding(targetBlockId, targetInputName, FlowInputSourceKind.Constant, null, null, value, null);
        }

        public static FlowInputBinding FromFlowValue(string flowValueName, string targetBlockId, string targetInputName) {
            return new FlowInputBinding(targetBlockId, targetInputName, FlowInputSourceKind.FlowValue, null, null, null, flowValueName);
        }
    }

    public sealed class FlowBlockNode {
        public string BlockId { get; set; }
        public IBlock Block { get; set; }
        public decimal PositionX { get; set; } = 0;
        public decimal PositionY { get; set; } = 0;

        public string BlockTypeName => Block?.GetType().Name;

        public FlowBlockNode() { }

        public FlowBlockNode(string blockId, IBlock block) {
            BlockId = blockId;
            Block = block;
        }
    }

    public sealed class Flow {
        public const string ExecutionFlowValueName = "Execution";

        private readonly Dictionary<string, FlowBlockNode> _blocks = new Dictionary<string, FlowBlockNode>(StringComparer.Ordinal);
        private readonly Dictionary<FlowInputKey, FlowInputBinding> _inputBindings = new Dictionary<FlowInputKey, FlowInputBinding>();

        public Flow() { }

        public IReadOnlyCollection<FlowBlockNode> Blocks => _blocks.Values.ToArray();
        public IReadOnlyCollection<FlowInputBinding> InputBindings => _inputBindings.Values.ToArray();

        public void AddBlock(string blockId, IBlock block) {
            if (string.IsNullOrWhiteSpace(blockId)) {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(blockId));
            }

            if (block == null) {
                throw new ArgumentNullException(nameof(block));
            }

            if (_blocks.ContainsKey(blockId)) {
                throw new InvalidOperationException($"Block '{blockId}' already exists in the flow.");
            }

            _blocks[blockId] = new FlowBlockNode(blockId, block);
        }

        public FlowBlockNode GetBlock(string blockId) {
            if (!_blocks.TryGetValue(blockId, out var node)) {
                throw new KeyNotFoundException($"Block '{blockId}' was not found in the flow.");
            }

            return node;
        }

        public bool TryGetBlock(string blockId, out FlowBlockNode blockNode) {
            return _blocks.TryGetValue(blockId, out blockNode);
        }

        public void Connect(string sourceBlockId, string sourceOutputName, string targetBlockId, string targetInputName) {
            EnsureBlockExists(sourceBlockId, nameof(sourceBlockId));
            EnsureBlockExists(targetBlockId, nameof(targetBlockId));
            EnsureMemberName(sourceOutputName, nameof(sourceOutputName));
            EnsureMemberName(targetInputName, nameof(targetInputName));

            var binding = FlowInputBinding.FromConnection(sourceBlockId, sourceOutputName, targetBlockId, targetInputName);
            SetInputBinding(binding);
        }

        public void SetConstant(string targetBlockId, string targetInputName, object value) {
            EnsureBlockExists(targetBlockId, nameof(targetBlockId));
            EnsureMemberName(targetInputName, nameof(targetInputName));

            var binding = FlowInputBinding.FromConstant(targetBlockId, targetInputName, value);
            SetInputBinding(binding);
        }

        public void BindFlowValue(string flowValueName, string targetBlockId, string targetInputName) {
            EnsureMemberName(flowValueName, nameof(flowValueName));
            EnsureBlockExists(targetBlockId, nameof(targetBlockId));
            EnsureMemberName(targetInputName, nameof(targetInputName));

            var binding = FlowInputBinding.FromFlowValue(flowValueName, targetBlockId, targetInputName);
            SetInputBinding(binding);
        }

        public bool TryGetInputBinding(string targetBlockId, string targetInputName, out FlowInputBinding inputBinding) {
            var key = new FlowInputKey(targetBlockId, targetInputName);
            return _inputBindings.TryGetValue(key, out inputBinding);
        }

        internal IReadOnlyDictionary<string, FlowInputBinding> GetInputBindingsForBlock(string blockId) {
            return _inputBindings
                .Where(x => x.Key.BlockId.Equals(blockId, StringComparison.Ordinal))
                .ToDictionary(x => x.Key.InputName, x => x.Value, StringComparer.Ordinal);
        }

        private void SetInputBinding(FlowInputBinding binding) {
            var key = new FlowInputKey(binding.TargetBlockId, binding.TargetInputName);
            _inputBindings[key] = binding;
        }

        private void EnsureBlockExists(string blockId, string paramName) {
            if (string.IsNullOrWhiteSpace(blockId)) {
                throw new ArgumentException("Value cannot be null or whitespace.", paramName);
            }

            if (!_blocks.ContainsKey(blockId)) {
                throw new InvalidOperationException($"Block '{blockId}' was not found in the flow.");
            }
        }

        private static void EnsureMemberName(string memberName, string paramName) {
            if (string.IsNullOrWhiteSpace(memberName)) {
                throw new ArgumentException("Value cannot be null or whitespace.", paramName);
            }
        }

        private readonly struct FlowInputKey : IEquatable<FlowInputKey> {
            public string BlockId { get; }
            public string InputName { get; }

            public FlowInputKey(string blockId, string inputName) {
                BlockId = blockId;
                InputName = inputName;
            }

            public bool Equals(FlowInputKey other) {
                return string.Equals(BlockId, other.BlockId, StringComparison.Ordinal)
                    && string.Equals(InputName, other.InputName, StringComparison.Ordinal);
            }

            public override bool Equals(object obj) {
                return obj is FlowInputKey other && Equals(other);
            }

            public override int GetHashCode() {
                unchecked {
                    return ((BlockId != null ? StringComparer.Ordinal.GetHashCode(BlockId) : 0) * 397) ^ (InputName != null ? StringComparer.Ordinal.GetHashCode(InputName) : 0);
                }
            }
        }
    }
}
