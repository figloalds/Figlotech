using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Figlotech.DataFlow {

    public sealed class FlowJsonConverter : JsonConverter<Flow> {
        private readonly IReadOnlyDictionary<string, Type> _availableBlocksByName;

        public FlowJsonConverter(IEnumerable<Type> availableBlocks) {
            if (availableBlocks == null) {
                throw new ArgumentNullException(nameof(availableBlocks));
            }

            var map = new Dictionary<string, Type>(StringComparer.Ordinal);
            foreach (var type in availableBlocks) {
                if (type == null) {
                    continue;
                }

                if (!typeof(IBlock).IsAssignableFrom(type)) {
                    throw new ArgumentException($"Type '{type.FullName}' does not implement IBlock.", nameof(availableBlocks));
                }

                if (type.IsAbstract || type.IsInterface) {
                    throw new ArgumentException($"Type '{type.FullName}' cannot be abstract or interface.", nameof(availableBlocks));
                }

                if (map.ContainsKey(type.Name)) {
                    throw new ArgumentException($"Duplicate block type name '{type.Name}' in available block list.", nameof(availableBlocks));
                }

                map[type.Name] = type;
            }

            _availableBlocksByName = map;
        }

        public override void WriteJson(JsonWriter writer, Flow value, JsonSerializer serializer) {
            if (value == null) {
                writer.WriteNull();
                return;
            }

            var root = new JObject();
            var blocks = new JArray();
            foreach (var node in value.Blocks) {
                if (node == null || node.Block == null) {
                    continue;
                }

                var blockObj = new JObject {
                    [nameof(FlowBlockNode.BlockId)] = node.BlockId,
                    [nameof(FlowBlockNode.BlockTypeName)] = node.BlockTypeName,
                    [nameof(FlowBlockNode.Block)] = JToken.FromObject(node.Block, serializer),
                };

                blocks.Add(blockObj);
            }

            root[nameof(Flow.Blocks)] = blocks;
            root[nameof(Flow.InputBindings)] = JArray.FromObject(value.InputBindings, serializer);
            root.WriteTo(writer);
        }

        public override Flow ReadJson(JsonReader reader, Type objectType, Flow existingValue, bool hasExistingValue, JsonSerializer serializer) {
            if (reader.TokenType == JsonToken.Null) {
                return null;
            }

            var root = JObject.Load(reader);
            var flow = new Flow();

            var blockArray = root[nameof(Flow.Blocks)] as JArray;
            if (blockArray == null) {
                throw new JsonSerializationException($"Missing '{nameof(Flow.Blocks)}' when loading flow.");
            }

            foreach (var token in blockArray.OfType<JObject>()) {
                var blockId = token[nameof(FlowBlockNode.BlockId)]?.ToObject<string>();
                var blockTypeName = token[nameof(FlowBlockNode.BlockTypeName)]?.ToObject<string>();

                if (string.IsNullOrWhiteSpace(blockId)) {
                    throw new JsonSerializationException("Flow block is missing BlockId.");
                }

                if (string.IsNullOrWhiteSpace(blockTypeName)) {
                    throw new JsonSerializationException($"Flow block '{blockId}' is missing BlockTypeName.");
                }

                if (!_availableBlocksByName.TryGetValue(blockTypeName, out var blockType)) {
                    throw new JsonSerializationException($"Flow block '{blockId}' failed to load. Block type '{blockTypeName}' is not available.");
                }

                var blockToken = token[nameof(FlowBlockNode.Block)];
                var block = blockToken == null || blockToken.Type == JTokenType.Null
                    ? (IBlock)Activator.CreateInstance(blockType)
                    : (IBlock)blockToken.ToObject(blockType, serializer);

                if (block == null) {
                    throw new JsonSerializationException($"Flow block '{blockId}' failed to load as type '{blockTypeName}'.");
                }

                flow.AddBlock(blockId, block);
            }

            var bindingsToken = root[nameof(Flow.InputBindings)] ?? root["Connections"];
            if (bindingsToken is JArray bindingsArray) {
                foreach (var token in bindingsArray.OfType<JObject>()) {
                    var binding = ReadBinding(token, serializer);
                    ApplyBinding(flow, binding);
                }
            }

            return flow;
        }

        private static FlowInputBinding ReadBinding(JObject token, JsonSerializer serializer) {
            var sourceKind = token[nameof(FlowInputBinding.SourceKind)]?.ToObject<FlowInputSourceKind>()
                ?? throw new JsonSerializationException("Flow input binding is missing SourceKind.");

            var binding = new FlowInputBinding {
                SourceKind = sourceKind,
                TargetBlockId = token[nameof(FlowInputBinding.TargetBlockId)]?.ToObject<string>(),
                TargetInputName = token[nameof(FlowInputBinding.TargetInputName)]?.ToObject<string>(),
                SourceBlockId = token[nameof(FlowInputBinding.SourceBlockId)]?.ToObject<string>(),
                SourceOutputName = token[nameof(FlowInputBinding.SourceOutputName)]?.ToObject<string>(),
                FlowValueName = token[nameof(FlowInputBinding.FlowValueName)]?.ToObject<string>(),
                ConstantValue = UnwrapValue(token[nameof(FlowInputBinding.ConstantValue)], serializer),
            };

            return binding;
        }

        private static object UnwrapValue(JToken token, JsonSerializer serializer) {
            if (token == null || token.Type == JTokenType.Null) {
                return null;
            }

            if (token is JValue jValue) {
                return jValue.Value;
            }

            return token.ToObject<object>(serializer);
        }

        private static void ApplyBinding(Flow flow, FlowInputBinding binding) {
            switch (binding.SourceKind) {
                case FlowInputSourceKind.Connection:
                    flow.Connect(binding.SourceBlockId, binding.SourceOutputName, binding.TargetBlockId, binding.TargetInputName);
                    return;
                case FlowInputSourceKind.Constant:
                    flow.SetConstant(binding.TargetBlockId, binding.TargetInputName, binding.ConstantValue);
                    return;
                case FlowInputSourceKind.FlowValue:
                    flow.BindFlowValue(binding.FlowValueName, binding.TargetBlockId, binding.TargetInputName);
                    return;
                default:
                    throw new JsonSerializationException($"Unknown binding source kind '{binding.SourceKind}'.");
            }
        }
    }
}
