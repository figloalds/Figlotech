using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Figlotech.Core.Fibton {
    public enum FibtonType {
        Null = 0,
        Object = 1,
        Array = 2,
        String = 3,
        Int32 = 4,
        Int64 = 5,
        Float32 = 6,
        Float64 = 7,
        Decimal = 8,
        Boolean = 9,
    }

    public sealed class FibtonIgnore : Attribute {

    }

    public sealed class FibtonProperty {
        public string Name { get; set; }
        public FibtonType Type { get; set; }
    }
    
    internal sealed class FibtonTypeDefCacheEntry {
        public int TypeId { get; set; }
        public Type ObjectType  { get; set; }
        public FibtonProperty[] Properties { get; set; }
    }

    public class FibtonConvert {
        public static long _serializeObject_Internal(object obj, Stream stream, List<FibtonTypeDefCacheEntry> typeCache, bool isRoot) {
            if(obj == null) {
                stream.Write(BitConverter.GetBytes((sbyte) FibtonType.Null));
                return sizeof(sbyte);
            }
            if(obj is string str) {
                stream.Write(BitConverter.GetBytes((sbyte) FibtonType.String));
                var strBytes = Encoding.UTF8.GetBytes(str);
                stream.Write(BitConverter.GetBytes(strBytes.Length));
                stream.Write(strBytes);
                return sizeof(sbyte) + sizeof(int) + strBytes.Length;
            }
            if(obj is int int32) {
                stream.Write(BitConverter.GetBytes((sbyte) FibtonType.Int32));
                stream.Write(BitConverter.GetBytes(int32));
                return sizeof(sbyte) + sizeof(int);
            }
            if(obj is long int64) {
                stream.Write(BitConverter.GetBytes((sbyte) FibtonType.Int64));
                stream.Write(BitConverter.GetBytes(int64));
                return sizeof(sbyte) + sizeof(long);
            }
            if (obj is float f) {
                stream.Write(BitConverter.GetBytes((sbyte) FibtonType.Float32));
                stream.Write(BitConverter.GetBytes(f));
                return sizeof(sbyte) + sizeof(float);
            }
            if(obj is double d) {
                stream.Write(BitConverter.GetBytes((sbyte) FibtonType.Float64));
                stream.Write(BitConverter.GetBytes(d));
                return sizeof(sbyte) + sizeof(double);
            }
            if (obj is decimal dec) {
                stream.Write(BitConverter.GetBytes((sbyte) FibtonType.Decimal));
                var bits = decimal.GetBits(dec);
                foreach(var bit in bits) {
                    stream.Write(BitConverter.GetBytes(bit));
                }
                return sizeof(sbyte) + sizeof(int) * 4;
            }


            using var ms = new MemoryStream();
            if (obj.GetType().Implements(typeof(IEnumerable<>))) {
                var ulType = obj.GetType()
                    .GetInterfaces()
                    .FirstOrDefault(x=> x == typeof(IEnumerable<>))
                    .GetGenericArguments()[0];
                if (ulType != null) {
                    var def = typeCache.FirstOrDefault(x => x.ObjectType == ulType);
                    if(!isRoot && def == null) {
                        throw new Exception("Error serializing array of unknown type " + ulType.FullName);
                    }
                    if(def == null) {
                        typeCache.Add(new FibtonTypeDefCacheEntry {
                            TypeId = typeCache.Count + 1,
                            ObjectType = ulType,
                            Properties = _getObjectProperties(ulType)
                        });
                    }
                    var outputStream = isRoot ? ms : stream;

                    outputStream.Write(BitConverter.GetBytes((sbyte)FibtonType.Array));
                    outputStream.Write(BitConverter.GetBytes(def.TypeId));

                    long totalElementBytes = 0;
                    var enumerable = (System.Collections.IEnumerable)obj;
                    var elementCount = 0;

                    return sizeof(sbyte) + sizeof(int) + totalElementBytes;
                }
            }

            return 0;
        }

        public static FibtonType _getFibtonType(Type type) {
            if (type == typeof(string)) {
                return FibtonType.String;
            }
            if (type == typeof(int)) {
                return FibtonType.Int32;
            }
            if (type == typeof(long)) {
                return FibtonType.Int64;
            }
            if (type == typeof(float)) {
                return FibtonType.Float32;
            }
            if (type == typeof(double)) {
                return FibtonType.Float64;
            }
            if (type == typeof(decimal)) {
                return FibtonType.Decimal;
            }
            if (type == typeof(bool)) {
                return FibtonType.Boolean;
            }
            if (type.Implements(typeof(IEnumerable<object>))) {
                return FibtonType.Array;
            }
            return FibtonType.Object;
        }

        public static FibtonProperty[] _getObjectProperties(Type obj) {
            var props = ReflectionTool.FieldsAndPropertiesOf(obj);
            var fibtonProps = new List<FibtonProperty>();

            foreach (var prop in props) {
                if (prop.GetCustomAttribute<FibtonIgnore>() != null) {
                    continue;
                }
                var propType = ReflectionTool.GetTypeOf(prop);
                if (Nullable.GetUnderlyingType(propType) != null) {
                    propType = Nullable.GetUnderlyingType(propType);
                }
                var fibtonType = _getFibtonType(propType);
                fibtonProps.Add(new FibtonProperty {
                    Name = prop.Name,
                    Type = fibtonType
                });
            }

            return fibtonProps.ToArray();
        }

        public static long _writeObjectMetadata(FibtonProperty[] properties, Stream stream) {
            using var ms = new MemoryStream();
            foreach (var prop in properties) {
                _serializeObject_Internal(prop.Name, ms, false);
                ms.WriteByte((byte)prop.Type);
            }
            stream.Write(BitConverter.GetBytes((int)ms.Length));
            stream.Write(ms.ToArray());
            return sizeof(int) + ms.Length;
        }

        public static byte[] SerializeObject(object obj) {
            using var ms = new MemoryStream();
            _serializeObject_Internal(obj, ms, true);
            return ms.ToArray();
        }

        public static T DeserializeObject<T>(byte[] data) {

        }
    }
}
