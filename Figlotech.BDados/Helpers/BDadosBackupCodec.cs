using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core.Interfaces;

namespace Figlotech.BDados.Helpers {
    /// <summary>Binds a schema once, then reads/writes rows without repeating schema or reflection discovery.</summary>
    internal sealed class BDadosBackupCodec {
        internal static readonly Encoding Utf8 = new UTF8Encoding(false, true);
        private readonly Type _type;
        private readonly MemberInfo[] _members;
        private readonly Type[] _valueTypes;
        private readonly BDadosBackupDataType[] _dataTypes;
        private readonly bool[] _nullable;
        private readonly bool[] _targetNullable;
        private readonly int?[] _fixedLengths;
        private readonly BDadosBackupBinaryType[] _binaryTypes;
        private readonly string[] _columnNames;
        private readonly bool _forImport;
        private readonly (MemberInfo Member, object Value)[] _missingDefaults;

        internal BDadosBackupCodec(Type type, BDadosBackupSchema schema, bool forImport = false) {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(schema);
            schema.Validate();
            if (!type.IsClass || type.IsAbstract || type.ContainsGenericParameters || type.GetConstructor(Type.EmptyTypes) == null) {
                throw new ArgumentException("Backup models must be concrete classes with a public parameterless constructor.", nameof(type));
            }
            var members = BDadosBackupSchema.GetMembers(type);
            if (!forImport && members.Length != schema.Columns.Count) {
                throw new InvalidDataException($"Schema for '{schema.TableName}' does not match '{type.FullName}'.");
            }
            _type = type;
            _forImport = forImport;
            _members = new MemberInfo[schema.Columns.Count];
            _valueTypes = new Type[_members.Length];
            _dataTypes = new BDadosBackupDataType[_members.Length];
            _nullable = new bool[_members.Length];
            _targetNullable = new bool[_members.Length];
            _fixedLengths = new int?[_members.Length];
            _binaryTypes = new BDadosBackupBinaryType[_members.Length];
            _columnNames = schema.Columns.Select(c => c.Name).ToArray();
            var mapped = forImport ? BDadosBackupMapping.Bind(_columnNames, members, m => m.Name,
                m => m.GetCustomAttribute<OldNameAttribute>(true)?.Name) : null;
            for (int i = 0; i < _members.Length; i++) {
                var column = schema.Columns[i];
                _dataTypes[i] = column.DataType;
                _nullable[i] = column.IsNullable;
                _fixedLengths[i] = column.FixedLength;
                var member = forImport ? mapped[i] : members.SingleOrDefault(m => m.Name == column.Name);
                if (member == null && forImport) continue; // Removed column: consume its bytes without assigning it.
                if (member == null) {
                    throw new InvalidDataException($"Missing member '{type.Name}.{column.Name}'.");
                }
                var memberType = BDadosBackupSchema.GetMemberType(member);
                var valueType = Nullable.GetUnderlyingType(memberType) ?? memberType;
                bool nullable = !memberType.IsValueType || Nullable.GetUnderlyingType(memberType) != null;
                if (forImport ? !BDadosBackupValueConverter.CanConvert(column.DataType, GetDataType(valueType))
                    : GetDataType(valueType) != column.DataType || nullable != column.IsNullable
                        || (valueType.IsEnum ? valueType.FullName : null) != column.EnumType) {
                    throw new InvalidDataException($"Incompatible member '{type.Name}.{column.Name}'.");
                }
                if (column.DataType == BDadosBackupDataType.BinarySerializable) {
                    var binaryType = BDadosBackupBinaryType.Get(valueType);
                    if (column.BinaryType != valueType.FullName || column.FixedLength != binaryType.FixedLength) {
                        throw new InvalidDataException($"Incompatible binary metadata for '{type.Name}.{column.Name}'.");
                    }
                    _binaryTypes[i] = binaryType;
                }
                _members[i] = member;
                _valueTypes[i] = valueType;
                _targetNullable[i] = nullable;
            }
            // Defaults belong to the current model, not the archive. Resolve and convert them
            // once per binding, and only for members absent after applying rename mappings.
            _missingDefaults = forImport ? members.Where(m => !_members.Contains(m))
                .Select(m => (Member: m, Field: m.GetCustomAttribute<FieldAttribute>(true)))
                .Where(entry => entry.Field.HasDefaultValue)
                .Select(entry => (entry.Member, ConvertDefault(entry.Member, entry.Field.DefaultValue))).ToArray()
                : Array.Empty<(MemberInfo, object)>();
        }

        private static object ConvertDefault(MemberInfo member, object value) {
            var memberType = BDadosBackupSchema.GetMemberType(member);
            var valueType = Nullable.GetUnderlyingType(memberType) ?? memberType;
            try {
                if (value == null) {
                    if (memberType.IsValueType && Nullable.GetUnderlyingType(memberType) == null) {
                        throw new InvalidDataException("A non-nullable value type cannot have a null default.");
                    }
                    return null;
                }
                if (!BDadosBackupValueConverter.CanConvert(GetDataType(value.GetType()), GetDataType(valueType))) {
                    throw new InvalidDataException("The default cannot be converted to the member's type.");
                }
                return BDadosBackupValueConverter.ConvertValue(value, valueType);
            } catch (Exception exception) when (exception is ArgumentException || exception is FormatException
                || exception is OverflowException || exception is InvalidCastException || exception is InvalidDataException
                || exception is NotSupportedException) {
                throw new InvalidDataException($"Invalid Field.DefaultValue for new member '{member.DeclaringType?.Name}.{member.Name}' ({memberType.Name}).", exception);
            }
        }

        internal byte[] Export(object instance, int maxBytes = int.MaxValue) {
            ArgumentNullException.ThrowIfNull(instance);
            if (_forImport) throw new InvalidOperationException("An import binding cannot export rows.");
            if (!_type.IsInstanceOfType(instance)) {
                throw new ArgumentException($"Expected an instance of '{_type.FullName}'.", nameof(instance));
            }
            using var buffer = new MemoryStream();
            using var writer = new BinaryWriter(buffer, Utf8, true);
            for (int i = 0; i < _members.Length; i++) {
                object value = _members[i] is PropertyInfo property ? property.GetValue(instance) : ((FieldInfo)_members[i]).GetValue(instance);
                if (_binaryTypes[i] != null) {
                    WriteBinaryValue(writer, i, value, maxBytes);
                    continue;
                }
                if (_fixedLengths[i].HasValue) {
                    long fixedSize = (value == null ? 0 : _fixedLengths[i].Value) + (_nullable[i] ? 1L : 0L);
                    if (buffer.Position + fixedSize > maxBytes) {
                        throw new InvalidDataException("Row exceeds the configured byte limit.");
                    }
                    if (_nullable[i]) writer.Write((byte)(value == null ? 0 : 1));
                    if (value == null) continue;
                    if (_valueTypes[i].IsEnum) {
                        value = Convert.ChangeType(value, Enum.GetUnderlyingType(_valueTypes[i]), CultureInfo.InvariantCulture);
                    }
                    WriteValue(writer, _dataTypes[i], value);
                    continue;
                }
                if (buffer.Length + 4 > maxBytes) {
                    throw new InvalidDataException("Row exceeds the configured byte limit.");
                }
                if (value == null) {
                    writer.Write(-1);
                    continue;
                }
                // Only this per-row memory buffer is seekable; the caller's stream never needs to be.
                long lengthOffset = buffer.Position;
                writer.Write(0);
                long start = buffer.Position;
                if (_valueTypes[i].IsEnum) {
                    value = Convert.ChangeType(value, Enum.GetUnderlyingType(_valueTypes[i]), CultureInfo.InvariantCulture);
                }
                long size = value is byte[] bytes ? bytes.Length : value is string text ? Utf8.GetByteCount(text) : 16;
                if ((value is byte[] || value is string) && buffer.Length + size > maxBytes) {
                    throw new InvalidDataException("Row exceeds the configured byte limit.");
                }
                WriteValue(writer, _dataTypes[i], value);
                long end = buffer.Position;
                if (end > maxBytes) {
                    throw new InvalidDataException("Row exceeds the configured byte limit.");
                }
                buffer.Position = lengthOffset;
                writer.Write(checked((int)(end - start)));
                buffer.Position = end;
            }
            return buffer.ToArray();
        }

        private void WriteBinaryValue(BinaryWriter writer, int index, object value, int maxBytes) {
            int? fixedLength = _fixedLengths[index];
            int prefixLength = fixedLength.HasValue ? (_nullable[index] ? 1 : 0) : 4;
            if (writer.BaseStream.Position + prefixLength + (value != null ? fixedLength.GetValueOrDefault() : 0L) > maxBytes) {
                throw new InvalidDataException("Row exceeds the configured byte limit.");
            }
            if (value == null) {
                if (!_nullable[index]) throw new InvalidDataException($"Null value for '{_columnNames[index]}'.");
                if (fixedLength.HasValue) writer.Write((byte)0);
                else writer.Write(-1);
                return;
            }
            byte[] payload = ((IBinarySerializable)value).ToBytes();
            if (payload == null || fixedLength.HasValue && payload.Length != fixedLength.Value) {
                throw new InvalidDataException($"Invalid binary payload length for '{_columnNames[index]}'.");
            }
            if (writer.BaseStream.Position + prefixLength + payload.Length > maxBytes) {
                throw new InvalidDataException("Row exceeds the configured byte limit.");
            }
            if (!fixedLength.HasValue) writer.Write(payload.Length);
            else if (_nullable[index]) writer.Write((byte)1);
            writer.Write(payload);
        }

        internal object Import(byte[] bytes) {
            ArgumentNullException.ThrowIfNull(bytes);
            using var buffer = new MemoryStream(bytes, false);
            using var reader = new BinaryReader(buffer, Utf8, true);
            object instance = Activator.CreateInstance(_type);
            try {
                for (int i = 0; i < _members.Length; i++) {
                    int length;
                    if (_fixedLengths[i].HasValue) {
                        length = _fixedLengths[i].Value;
                        if (_nullable[i]) {
                            length = reader.ReadByte() switch {
                                0 => -1, 1 => length,
                                _ => throw new InvalidDataException($"Invalid null marker for '{_columnNames[i]}'.")
                            };
                        }
                    } else {
                        length = reader.ReadInt32();
                    }
                    object value;
                    if (length == -1 && _nullable[i]) {
                        value = null;
                    } else {
                        if (length < 0 || length > buffer.Length - buffer.Position) {
                            throw new InvalidDataException($"Invalid length for '{_columnNames[i]}'.");
                        }
                        long end = buffer.Position + length;
                        if (_members[i] == null) {
                            buffer.Position = end;
                            continue;
                        }
                        if (_binaryTypes[i] != null) {
                            var binaryValue = _binaryTypes[i].CreateInstance();
                            try {
                                // Keep the interface box so mutations made by struct implementations survive.
                                binaryValue.FromBytes(reader.ReadBytes(length));
                            } catch (Exception exception) when (exception is ArgumentException || exception is FormatException
                                || exception is OverflowException || exception is InvalidDataException) {
                                throw new InvalidDataException($"Invalid binary value for '{_type.Name}.{_columnNames[i]}'.", exception);
                            }
                            value = binaryValue;
                        } else {
                            value = ReadValue(reader, _dataTypes[i], length);
                        }
                        if (buffer.Position != end) {
                            throw new InvalidDataException($"Invalid value length for '{_columnNames[i]}'.");
                        }
                    }
                    // Old NULLs for now non-nullable value types keep initializer/constructor
                    // defaults. Field.DefaultValue applies only when the column is absent.
                    if (_members[i] == null || value == null && !_targetNullable[i]) continue;
                    if (value != null) {
                        try {
                            value = BDadosBackupValueConverter.ConvertValue(value, _valueTypes[i]);
                        } catch (Exception exception) when (exception is ArgumentException || exception is FormatException
                            || exception is OverflowException || exception is InvalidCastException || exception is InvalidDataException) {
                            throw new InvalidDataException($"Cannot restore column '{_columnNames[i]}' into '{_type.Name}.{_members[i].Name}' ({_valueTypes[i].Name}).", exception);
                        }
                    }
                    if (_members[i] is PropertyInfo property) {
                        property.SetValue(instance, value);
                    } else {
                        ((FieldInfo)_members[i]).SetValue(instance, value);
                    }
                }
                if (buffer.Position != buffer.Length) {
                    throw new InvalidDataException("Unexpected bytes after the row's last field.");
                }
                foreach (var entry in _missingDefaults) {
                    // Attribute array values must not become shared mutable state across rows.
                    object value = entry.Value is Array array ? array.Clone() : entry.Value;
                    if (entry.Member is PropertyInfo property) {
                        property.SetValue(instance, value);
                    } else {
                        ((FieldInfo)entry.Member).SetValue(instance, value);
                    }
                }
                return instance;
            } catch (EndOfStreamException exception) {
                throw new InvalidDataException("Truncated row.", exception);
            } catch (ArgumentException exception) {
                throw new InvalidDataException("Invalid binary field value.", exception);
            }
        }

        internal static BDadosBackupDataType GetDataType(Type type) {
            if (typeof(IBinarySerializable).IsAssignableFrom(type)) return BDadosBackupDataType.BinarySerializable;
            if (type.IsEnum) type = Enum.GetUnderlyingType(type);
            if (type == typeof(byte[])) return BDadosBackupDataType.Bytes;
            if (type == typeof(Guid)) return BDadosBackupDataType.Guid;
            if (type == typeof(DateTimeOffset)) return BDadosBackupDataType.DateTimeOffset;
            if (type == typeof(TimeSpan)) return BDadosBackupDataType.TimeSpan;
            if (type == typeof(DateOnly)) return BDadosBackupDataType.DateOnly;
            if (type == typeof(TimeOnly)) return BDadosBackupDataType.TimeOnly;
            return Type.GetTypeCode(type) switch {
                TypeCode.Boolean => BDadosBackupDataType.Boolean,
                TypeCode.Byte => BDadosBackupDataType.Byte,
                TypeCode.SByte => BDadosBackupDataType.SByte,
                TypeCode.Int16 => BDadosBackupDataType.Int16,
                TypeCode.UInt16 => BDadosBackupDataType.UInt16,
                TypeCode.Int32 => BDadosBackupDataType.Int32,
                TypeCode.UInt32 => BDadosBackupDataType.UInt32,
                TypeCode.Int64 => BDadosBackupDataType.Int64,
                TypeCode.UInt64 => BDadosBackupDataType.UInt64,
                TypeCode.Single => BDadosBackupDataType.Single,
                TypeCode.Double => BDadosBackupDataType.Double,
                TypeCode.Decimal => BDadosBackupDataType.Decimal,
                TypeCode.Char => BDadosBackupDataType.Char,
                TypeCode.String => BDadosBackupDataType.String,
                TypeCode.DateTime => BDadosBackupDataType.DateTime,
                _ => throw new NotSupportedException($"Backup does not support persisted type '{type.FullName}'.")
            };
        }

        internal static int? GetFixedLength(BDadosBackupDataType type) {
            return type switch {
                BDadosBackupDataType.Boolean or BDadosBackupDataType.Byte or BDadosBackupDataType.SByte => 1,
                BDadosBackupDataType.Int16 or BDadosBackupDataType.UInt16 or BDadosBackupDataType.Char => 2,
                BDadosBackupDataType.Int32 or BDadosBackupDataType.UInt32 or BDadosBackupDataType.Single or BDadosBackupDataType.DateOnly => 4,
                BDadosBackupDataType.Int64 or BDadosBackupDataType.UInt64 or BDadosBackupDataType.Double
                    or BDadosBackupDataType.TimeSpan or BDadosBackupDataType.TimeOnly => 8,
                BDadosBackupDataType.DateTime => 9, // Ticks plus Kind, preserving the existing value representation.
                BDadosBackupDataType.DateTimeOffset => 10, // Ticks plus offset minutes.
                BDadosBackupDataType.Decimal or BDadosBackupDataType.Guid => 16,
                _ => null
            };
        }

        private static void WriteValue(BinaryWriter writer, BDadosBackupDataType type, object value) {
            switch (type) {
                case BDadosBackupDataType.Boolean: writer.Write((bool)value); break;
                case BDadosBackupDataType.Byte: writer.Write((byte)value); break;
                case BDadosBackupDataType.SByte: writer.Write((sbyte)value); break;
                case BDadosBackupDataType.Int16: writer.Write((short)value); break;
                case BDadosBackupDataType.UInt16: writer.Write((ushort)value); break;
                case BDadosBackupDataType.Int32: writer.Write((int)value); break;
                case BDadosBackupDataType.UInt32: writer.Write((uint)value); break;
                case BDadosBackupDataType.Int64: writer.Write((long)value); break;
                case BDadosBackupDataType.UInt64: writer.Write((ulong)value); break;
                case BDadosBackupDataType.Single: writer.Write((float)value); break;
                case BDadosBackupDataType.Double: writer.Write((double)value); break;
                case BDadosBackupDataType.Decimal: writer.Write((decimal)value); break;
                case BDadosBackupDataType.Char: writer.Write((ushort)(char)value); break;
                case BDadosBackupDataType.String: writer.Write(Utf8.GetBytes((string)value)); break;
                case BDadosBackupDataType.Bytes: writer.Write((byte[])value); break;
                case BDadosBackupDataType.Guid: writer.Write(((Guid)value).ToByteArray()); break;
                case BDadosBackupDataType.DateTime:
                    var date = (DateTime)value;
                    writer.Write(date.Ticks);
                    writer.Write((byte)date.Kind);
                    break;
                case BDadosBackupDataType.DateTimeOffset:
                    var offset = (DateTimeOffset)value;
                    writer.Write(offset.Ticks);
                    writer.Write((short)offset.Offset.TotalMinutes);
                    break;
                case BDadosBackupDataType.TimeSpan: writer.Write(((TimeSpan)value).Ticks); break;
                case BDadosBackupDataType.DateOnly: writer.Write(((DateOnly)value).DayNumber); break;
                case BDadosBackupDataType.TimeOnly: writer.Write(((TimeOnly)value).Ticks); break;
                default: throw new InvalidDataException("Unknown backup data type.");
            }
        }

        private static object ReadValue(BinaryReader reader, BDadosBackupDataType type, int length) {
            return type switch {
                BDadosBackupDataType.Boolean => reader.ReadByte() switch {
                    0 => false, 1 => true, _ => throw new InvalidDataException("Invalid Boolean value.")
                },
                BDadosBackupDataType.Byte => reader.ReadByte(),
                BDadosBackupDataType.SByte => reader.ReadSByte(),
                BDadosBackupDataType.Int16 => reader.ReadInt16(),
                BDadosBackupDataType.UInt16 => reader.ReadUInt16(),
                BDadosBackupDataType.Int32 => reader.ReadInt32(),
                BDadosBackupDataType.UInt32 => reader.ReadUInt32(),
                BDadosBackupDataType.Int64 => reader.ReadInt64(),
                BDadosBackupDataType.UInt64 => reader.ReadUInt64(),
                BDadosBackupDataType.Single => reader.ReadSingle(),
                BDadosBackupDataType.Double => reader.ReadDouble(),
                BDadosBackupDataType.Decimal => reader.ReadDecimal(),
                BDadosBackupDataType.Char => (char)reader.ReadUInt16(),
                BDadosBackupDataType.String => Utf8.GetString(reader.ReadBytes(length)),
                BDadosBackupDataType.Bytes => reader.ReadBytes(length),
                BDadosBackupDataType.Guid => new Guid(reader.ReadBytes(16)),
                BDadosBackupDataType.DateTime => new DateTime(reader.ReadInt64(), (DateTimeKind)reader.ReadByte()),
                BDadosBackupDataType.DateTimeOffset => new DateTimeOffset(reader.ReadInt64(), TimeSpan.FromMinutes(reader.ReadInt16())),
                BDadosBackupDataType.TimeSpan => TimeSpan.FromTicks(reader.ReadInt64()),
                BDadosBackupDataType.DateOnly => DateOnly.FromDayNumber(reader.ReadInt32()),
                BDadosBackupDataType.TimeOnly => new TimeOnly(reader.ReadInt64()),
                _ => throw new InvalidDataException("Unknown backup data type.")
            };
        }
    }
}
