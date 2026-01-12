using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Figlotech.Core
{
    public static class ValuePathReader
    {
        static SelfInitializerDictionary<Type, LenientDictionary<string, PropertyInfo>> PropertyCache =
            new SelfInitializerDictionary<Type, LenientDictionary<string, PropertyInfo>>(
                t => {
                    return (LenientDictionary<string, PropertyInfo>)t
                        .GetProperties()
                        .ToDictionary(p => p.Name, p => p);
                }
            );
        static SelfInitializerDictionary<Type, MethodInfo> SquareMethodsStringCache =
            new SelfInitializerDictionary<Type, MethodInfo>(
                t => {
                    return t
                        .GetMethods()
                        .FirstOrDefault(
                            x => x.Name == "get_Item" && 
                                x.GetParameters().Length == 1 && 
                                x.GetParameters()[0].ParameterType == typeof(String)
                            );
                }
            ) {
                AllowNullValueCaching = true
            };
        static SelfInitializerDictionary<Type, MethodInfo> SquareMethodsIntCache =
            new SelfInitializerDictionary<Type, MethodInfo>(
                t => {
                    return t
                        .GetMethods()
                        .FirstOrDefault(
                            x => x.Name == "get_Item" && 
                                x.GetParameters().Length == 1 && 
                                x.GetParameters()[0].ParameterType == typeof(Int32)
                            );
                }
            ) {
                AllowNullValueCaching = true
            };

        private enum SegmentType { Property, Index }

        private struct PathSegment {
            public SegmentType Type;
            public string Value;
        }

        public static object Read(object input, string path) {
            if (input == null) return null;
            if (string.IsNullOrWhiteSpace(path)) return input;

            try {
                var segments = ParsePath(path);
                var current = input;

                foreach (var segment in segments) {
                    if (current == null) return null;

                    var type = current.GetType();

                    try {
                        if (segment.Type == SegmentType.Property) {
                            // Try property access first
                            var prop = PropertyCache[type][segment.Value];
                            if (prop != null) {
                                current = prop.GetValue(current);
                            } else {
                                // Try string indexer
                                var squareGetterString = SquareMethodsStringCache[type];
                                if (squareGetterString != null) {
                                    current = squareGetterString.Invoke(current, new object[] { segment.Value });
                                } else {
                                    return null;
                                }
                            }
                        } else if (segment.Type == SegmentType.Index) {
                            // Try int indexer first
                            if (Int32.TryParse(segment.Value, out var index)) {
                                var squareGetterInt = SquareMethodsIntCache[type];
                                if (squareGetterInt != null) {
                                    current = squareGetterInt.Invoke(current, new object[] { index });
                                    continue;
                                }
                            }

                            // Fall back to string indexer
                            var squareGetterString = SquareMethodsStringCache[type];
                            if (squareGetterString != null) {
                                current = squareGetterString.Invoke(current, new object[] { segment.Value });
                            } else {
                                return null;
                            }
                        }
                    }
                    catch (TargetInvocationException) {
                        return null;
                    } catch (IndexOutOfRangeException) {
                        return null;
                    } catch (ArgumentOutOfRangeException) {
                        return null;
                    }
                }
                if (current != null && current.GetType().IsEnum) {
                    return (int)current;
                }

                return current;
            } catch {
                return null;
            }
        }

        private static List<PathSegment> ParsePath(string path) {
            var segments = new List<PathSegment>();
            var current = new StringBuilder();
            bool inBracket = false;

            for (int i = 0; i < path.Length; i++) {
                char ch = path[i];

                if (ch == '[') {
                    if (current.Length > 0) {
                        var value = current.ToString().Trim();
                        if (!string.IsNullOrEmpty(value)) {
                            segments.Add(new PathSegment { Type = SegmentType.Property, Value = value });
                        }
                        current.Clear();
                    }
                    inBracket = true;
                } else if (ch == ']') {
                    if (inBracket && current.Length > 0) {
                        var value = current.ToString().Trim();
                        if (!string.IsNullOrEmpty(value)) {
                            segments.Add(new PathSegment { Type = SegmentType.Index, Value = value });
                        }
                        current.Clear();
                    }
                    inBracket = false;
                } else if (ch == '.' && !inBracket) {
                    if (current.Length > 0) {
                        var value = current.ToString().Trim();
                        if (!string.IsNullOrEmpty(value)) {
                            segments.Add(new PathSegment { Type = SegmentType.Property, Value = value });
                        }
                        current.Clear();
                    }
                } else {
                    current.Append(ch);
                }
            }

            if (current.Length > 0) {
                var value = current.ToString().Trim();
                if (!string.IsNullOrEmpty(value)) {
                    segments.Add(new PathSegment { Type = SegmentType.Property, Value = value });
                }
            }

            return segments;
        }
    }
}
