using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.DataFlow {

    public sealed class FlowRunResult {
        public string EntryBlockId { get; }
        public IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> OutputsByBlock { get; }

        public FlowRunResult(string entryBlockId, IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> outputsByBlock) {
            EntryBlockId = entryBlockId;
            OutputsByBlock = outputsByBlock;
        }

        public bool TryGetOutput(string blockId, string outputName, out object value) {
            value = null;

            if (!OutputsByBlock.TryGetValue(blockId, out var blockOutputs)) {
                return false;
            }

            return blockOutputs.TryGetValue(outputName, out value);
        }
    }

    public sealed class FlowRunner {
        private static readonly ConcurrentDictionary<Type, BlockMetadata> MetadataByType = new ConcurrentDictionary<Type, BlockMetadata>();

        public async ValueTask<FlowRunResult> ExecuteAsync(Flow flow, string entryBlockId, object executionTick = null, IReadOnlyDictionary<string, object> flowValues = null, CancellationToken cancellationToken = default) {
            if (flow == null) {
                throw new ArgumentNullException(nameof(flow));
            }

            if (string.IsNullOrWhiteSpace(entryBlockId)) {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(entryBlockId));
            }

            flow.GetBlock(entryBlockId);

            var context = new RunnerContext(flow, BuildFlowValueMap(executionTick, flowValues), cancellationToken);
            await context.ExecuteBlockAsync(entryBlockId, new HashSet<string>(StringComparer.Ordinal)).ConfigureAwait(false);

            var outputsByBlock = context.Executions
                .ToDictionary(
                    kv => kv.Key,
                    kv => (IReadOnlyDictionary<string, object>)new ReadOnlyDictionary<string, object>(new Dictionary<string, object>(kv.Value.Outputs, StringComparer.Ordinal)),
                    StringComparer.Ordinal);

            return new FlowRunResult(entryBlockId, new ReadOnlyDictionary<string, IReadOnlyDictionary<string, object>>(outputsByBlock));
        }

        private static IReadOnlyDictionary<string, object> BuildFlowValueMap(object executionTick, IReadOnlyDictionary<string, object> flowValues) {
            var values = new Dictionary<string, object>(StringComparer.Ordinal);

            if (flowValues != null) {
                foreach (var pair in flowValues) {
                    values[pair.Key] = pair.Value;
                }
            }

            values[Flow.ExecutionFlowValueName] = executionTick;
            return values;
        }

        private sealed class RunnerContext {
            private readonly Flow _flow;
            private readonly IReadOnlyDictionary<string, object> _flowValues;
            private readonly CancellationToken _cancellationToken;
            private readonly object _gate = new object();
            private readonly Dictionary<string, Task<BlockExecutionState>> _executionTasks = new Dictionary<string, Task<BlockExecutionState>>(StringComparer.Ordinal);

            public IReadOnlyDictionary<string, BlockExecutionState> Executions {
                get {
                    var completed = new Dictionary<string, BlockExecutionState>(StringComparer.Ordinal);
                    foreach (var pair in _executionTasks) {
                        if (pair.Value.IsCompletedSuccessfully) {
                            completed[pair.Key] = pair.Value.Result;
                        }
                    }

                    return completed;
                }
            }

            public RunnerContext(Flow flow, IReadOnlyDictionary<string, object> flowValues, CancellationToken cancellationToken) {
                _flow = flow;
                _flowValues = flowValues;
                _cancellationToken = cancellationToken;
            }

            public Task<BlockExecutionState> ExecuteBlockAsync(string blockId, HashSet<string> stack) {
                if (stack.Contains(blockId)) {
                    var cycle = string.Join(" -> ", stack.Concat(new[] { blockId }));
                    throw new InvalidOperationException($"Cycle detected in flow execution: {cycle}");
                }

                lock (_gate) {
                    if (_executionTasks.TryGetValue(blockId, out var existingTask)) {
                        return existingTask;
                    }

                    var branchStack = new HashSet<string>(stack, StringComparer.Ordinal) { blockId };
                    var task = ExecuteBlockCoreAsync(blockId, branchStack);
                    _executionTasks[blockId] = task;
                    return task;
                }
            }

            private async Task<BlockExecutionState> ExecuteBlockCoreAsync(string blockId, HashSet<string> stack) {
                _cancellationToken.ThrowIfCancellationRequested();

                var node = _flow.GetBlock(blockId);
                var blockMetadata = GetBlockMetadata(node.Block.GetType());
                var blockBindings = _flow.GetInputBindingsForBlock(blockId);

                var dependencyBindingByInput = new Dictionary<string, FlowInputBinding>(StringComparer.Ordinal);
                var dependencyTasks = new List<Task<BlockExecutionState>>();

                foreach (var input in blockMetadata.Inputs.Values) {
                    if (blockBindings.TryGetValue(input.Name, out var binding) && binding.SourceKind == FlowInputSourceKind.Connection) {
                        dependencyBindingByInput[input.Name] = binding;
                        dependencyTasks.Add(ExecuteBlockAsync(binding.SourceBlockId, stack));
                    }
                }

                if (dependencyTasks.Count > 0) {
                    await Task.WhenAll(dependencyTasks).ConfigureAwait(false);
                }

                foreach (var input in blockMetadata.Inputs.Values) {
                    if (!blockBindings.TryGetValue(input.Name, out var binding)) {
                        if (input.Optional) {
                            continue;
                        }

                        var existingValue = input.GetValue(node.Block);
                        if (IsUnset(existingValue, input.InputType)) {
                            throw new InvalidOperationException($"Input '{input.Name}' of block '{blockId}' is not fulfilled.");
                        }

                        continue;
                    }

                    object resolvedValue;
                    switch (binding.SourceKind) {
                        case FlowInputSourceKind.Constant:
                            resolvedValue = binding.ConstantValue;
                            break;
                        case FlowInputSourceKind.FlowValue:
                            if (!_flowValues.TryGetValue(binding.FlowValueName, out resolvedValue)) {
                                throw new InvalidOperationException($"Flow value '{binding.FlowValueName}' required by '{blockId}.{input.Name}' was not provided.");
                            }
                            break;
                        case FlowInputSourceKind.Connection:
                            var dependency = await ExecuteBlockAsync(binding.SourceBlockId, stack).ConfigureAwait(false);
                            if (!dependency.Outputs.TryGetValue(binding.SourceOutputName, out resolvedValue)) {
                                throw new InvalidOperationException($"Output '{binding.SourceOutputName}' was not produced by block '{binding.SourceBlockId}'.");
                            }
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    input.SetValue(node.Block, resolvedValue);
                }

                await node.Block.Execute().ConfigureAwait(false);

                var outputs = new Dictionary<string, object>(StringComparer.Ordinal);
                foreach (var output in blockMetadata.Outputs.Values) {
                    outputs[output.Name] = await output.GetValue(node.Block).ConfigureAwait(false);
                }

                return new BlockExecutionState(outputs);
            }
        }

        private static bool IsUnset(object value, Type type) {
            if (value == null) {
                return true;
            }

            if (!type.IsValueType) {
                return false;
            }

            return value.Equals(Activator.CreateInstance(type));
        }

        private static BlockMetadata GetBlockMetadata(Type blockType) {
            return MetadataByType.GetOrAdd(blockType, BuildBlockMetadata);
        }

        private static BlockMetadata BuildBlockMetadata(Type blockType) {
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            var inputByName = new Dictionary<string, InputAccessor>(StringComparer.Ordinal);
            var outputByName = new Dictionary<string, OutputAccessor>(StringComparer.Ordinal);

            foreach (var property in blockType.GetProperties(flags)) {
                var inputAttribute = property.GetCustomAttribute<Input>();
                if (inputAttribute != null) {
                    if (!property.CanWrite) {
                        throw new InvalidOperationException($"Input '{blockType.Name}.{property.Name}' must be writable.");
                    }

                    inputByName[property.Name] = new InputAccessor(property.Name, property.PropertyType, inputAttribute.Optional, property.SetValue, property.GetValue);
                }

                var outputAttribute = property.GetCustomAttribute<Output>();
                if (outputAttribute != null) {
                    outputByName[property.Name] = new OutputAccessor(property.Name, b => new ValueTask<object>(property.GetValue(b)));
                }
            }

            foreach (var method in blockType.GetMethods(flags)) {
                var outputAttribute = method.GetCustomAttribute<Output>();
                if (outputAttribute == null) {
                    continue;
                }

                if (method.GetParameters().Length > 0) {
                    throw new InvalidOperationException($"Output method '{blockType.Name}.{method.Name}' cannot declare parameters.");
                }

                outputByName[method.Name] = new OutputAccessor(method.Name, b => InvokeOutputMethodAsync(method, b));
            }

            return new BlockMetadata(inputByName, outputByName);
        }

        private static async ValueTask<object> InvokeOutputMethodAsync(MethodInfo method, object block) {
            var result = method.Invoke(block, null);
            return await UnwrapTaskLikeAsync(result).ConfigureAwait(false);
        }

        private static async ValueTask<object> UnwrapTaskLikeAsync(object result) {
            if (result == null) {
                return null;
            }

            if (result is Task taskResult) {
                await taskResult.ConfigureAwait(false);
                return GetTaskResult(taskResult);
            }

            var type = result.GetType();
            if (type == typeof(ValueTask)) {
                await ((ValueTask)result).ConfigureAwait(false);
                return null;
            }

            if (type.IsValueType && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTask<>)) {
                var asTaskMethod = type.GetMethod(nameof(ValueTask<object>.AsTask), BindingFlags.Public | BindingFlags.Instance);
                var task = (Task)asTaskMethod.Invoke(result, null);
                await task.ConfigureAwait(false);
                return GetTaskResult(task);
            }

            return result;
        }

        private static object GetTaskResult(Task task) {
            var type = task.GetType();
            if (!type.IsGenericType) {
                return null;
            }

            var resultProperty = type.GetProperty("Result", BindingFlags.Public | BindingFlags.Instance);
            return resultProperty.GetValue(task);
        }

        private sealed class BlockExecutionState {
            public IReadOnlyDictionary<string, object> Outputs { get; }

            public BlockExecutionState(IReadOnlyDictionary<string, object> outputs) {
                Outputs = outputs;
            }
        }

        private sealed class BlockMetadata {
            public IReadOnlyDictionary<string, InputAccessor> Inputs { get; }
            public IReadOnlyDictionary<string, OutputAccessor> Outputs { get; }

            public BlockMetadata(IReadOnlyDictionary<string, InputAccessor> inputs, IReadOnlyDictionary<string, OutputAccessor> outputs) {
                Inputs = inputs;
                Outputs = outputs;
            }
        }

        private sealed class InputAccessor {
            private readonly Action<object, object> _setValue;
            private readonly Func<object, object> _getValue;

            public string Name { get; }
            public Type InputType { get; }
            public bool Optional { get; }

            public InputAccessor(string name, Type inputType, bool optional, Action<object, object> setValue, Func<object, object> getValue) {
                Name = name;
                InputType = inputType;
                Optional = optional;
                _setValue = setValue;
                _getValue = getValue;
            }

            public void SetValue(object instance, object value) {
                var converted = ConvertValue(value, InputType, Name);
                _setValue(instance, converted);
            }

            public object GetValue(object instance) {
                return _getValue(instance);
            }
        }

        private sealed class OutputAccessor {
            private readonly Func<object, ValueTask<object>> _getValue;

            public string Name { get; }

            public OutputAccessor(string name, Func<object, ValueTask<object>> getValue) {
                Name = name;
                _getValue = getValue;
            }

            public ValueTask<object> GetValue(object instance) {
                return _getValue(instance);
            }
        }

        private static object ConvertValue(object value, Type destinationType, string inputName) {
            if (value == null) {
                if (destinationType.IsValueType && Nullable.GetUnderlyingType(destinationType) == null) {
                    throw new InvalidOperationException($"Input '{inputName}' cannot receive null.");
                }

                return null;
            }

            var valueType = value.GetType();
            var targetType = Nullable.GetUnderlyingType(destinationType) ?? destinationType;
            if (targetType.IsAssignableFrom(valueType)) {
                return value;
            }

            if (targetType.IsEnum && value is string s) {
                return Enum.Parse(targetType, s, ignoreCase: true);
            }

            try {
                return Convert.ChangeType(value, targetType);
            } catch (Exception ex) {
                throw new InvalidOperationException($"Cannot convert value '{value}' ({valueType.Name}) to input '{inputName}' ({targetType.Name}).", ex);
            }
        }
    }
}
