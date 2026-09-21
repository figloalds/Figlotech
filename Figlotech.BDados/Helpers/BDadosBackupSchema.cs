using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using Figlotech.BDados.DataAccessAbstractions.Attributes;

namespace Figlotech.BDados.Helpers {
    /// <summary>A portable, ordered description of the persisted members of a model.</summary>
    public sealed class BDadosBackupSchema {
        public int Version { get; set; } = 1;
        public string TableName { get; set; }
        public string ModelName { get; set; }
        public List<BDadosBackupColumn> Columns { get; set; } = new List<BDadosBackupColumn>();

        public static BDadosBackupSchema Create<T>() => Create(typeof(T));

        public static BDadosBackupSchema Create(Type type) {
            ArgumentNullException.ThrowIfNull(type);
            var schema = new BDadosBackupSchema { Version = 2, TableName = type.Name, ModelName = type.FullName };
            foreach (var member in GetMembers(type).OrderBy(m => m.Name, StringComparer.Ordinal)) {
                var field = member.GetCustomAttribute<FieldAttribute>(true);
                var memberType = GetMemberType(member);
                var valueType = Nullable.GetUnderlyingType(memberType) ?? memberType;
                var foreignKey = member.GetCustomAttribute<ForeignKeyAttribute>(true);
                var dataType = BDadosBackupCodec.GetDataType(valueType);
                var binaryType = dataType == BDadosBackupDataType.BinarySerializable ? BDadosBackupBinaryType.Get(valueType) : null;
                schema.Columns.Add(new BDadosBackupColumn {
                    // BDados query generators use CLR type/member names as table/column names.
                    Name = member.Name,
                    DataType = dataType,
                    BinaryType = binaryType != null ? valueType.FullName : null,
                    FixedLength = binaryType != null ? binaryType.FixedLength : BDadosBackupCodec.GetFixedLength(dataType),
                    IsNullable = !memberType.IsValueType || Nullable.GetUnderlyingType(memberType) != null,
                    EnumType = valueType.IsEnum ? valueType.FullName : null,
                    DatabaseType = field.Type,
                    Size = field.Size,
                    Precision = field.Precision,
                    AllowNull = field.AllowNull,
                    IsPrimaryKey = field.PrimaryKey || member.IsDefined(typeof(PrimaryKeyAttribute), true),
                    IsReliableId = member.IsDefined(typeof(ReliableIdAttribute), true),
                    IsUnique = field.Unique,
                    IsUnsigned = field.Unsigned,
                    OldName = member.GetCustomAttribute<OldNameAttribute>(true)?.Name,
                    ForeignTable = foreignKey?.RefTable ?? foreignKey?.RefType?.Name,
                    ForeignColumn = foreignKey?.RefColumn
                });
            }
            schema.Validate();
            return schema;
        }

        public byte[] Serialize() {
            Validate();
            return JsonSerializer.SerializeToUtf8Bytes(this);
        }

        public static BDadosBackupSchema Deserialize(byte[] bytes) {
            ArgumentNullException.ThrowIfNull(bytes);
            try {
                var schema = JsonSerializer.Deserialize<BDadosBackupSchema>(bytes);
                if (schema == null) {
                    throw new InvalidDataException("Missing backup schema.");
                }
                schema.Validate();
                return schema;
            } catch (JsonException exception) {
                throw new InvalidDataException("Invalid backup schema JSON.", exception);
            }
        }

        internal void Validate() {
            if ((Version != 1 && Version != 2) || string.IsNullOrWhiteSpace(TableName) || Columns == null || Columns.Count == 0) {
                throw new InvalidDataException("Unsupported or incomplete backup schema.");
            }
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var column in Columns) {
                if (column == null || string.IsNullOrWhiteSpace(column.Name) || !names.Add(column.Name)
                    || !Enum.IsDefined(column.DataType)) {
                    throw new InvalidDataException($"Invalid or duplicate column in table '{TableName}'.");
                }
                if (column.DataType == BDadosBackupDataType.BinarySerializable
                    ? Version < 2 || string.IsNullOrWhiteSpace(column.BinaryType) || column.FixedLength < 0
                    : column.BinaryType != null || column.FixedLength != (Version == 1 ? null : BDadosBackupCodec.GetFixedLength(column.DataType))) {
                    throw new InvalidDataException($"Invalid binary metadata for '{TableName}.{column.Name}'.");
                }
            }
        }

        internal static MemberInfo[] GetMembers(Type type) {
            return type.GetMembers(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => m is FieldInfo field && !field.IsInitOnly
                    || m is PropertyInfo property && property.SetMethod != null)
                .Where(m => m.GetCustomAttribute<FieldAttribute>(true) != null).ToArray();
        }

        internal static Type GetMemberType(MemberInfo member) {
            if (member is FieldInfo field && !field.IsInitOnly) {
                return field.FieldType;
            }
            if (member is PropertyInfo property && property.GetIndexParameters().Length == 0
                && property.GetMethod != null && property.SetMethod != null) {
                return property.PropertyType;
            }
            throw new NotSupportedException($"Backup member '{member.DeclaringType?.Name}.{member.Name}' must be readable and writable.");
        }
    }

    public sealed class BDadosBackupColumn {
        public string Name { get; set; }
        public BDadosBackupDataType DataType { get; set; }
        public bool IsNullable { get; set; }
        public string EnumType { get; set; }
        /// <summary>CLR full name of a custom binary type; never used to load or resolve assemblies.</summary>
        public string BinaryType { get; set; }
        /// <summary>Payload bytes without a row-level length prefix; null means variable-length.</summary>
        public int? FixedLength { get; set; }
        public string DatabaseType { get; set; }
        public long Size { get; set; }
        public int Precision { get; set; }
        public bool AllowNull { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsReliableId { get; set; }
        public bool IsUnique { get; set; }
        public bool IsUnsigned { get; set; }
        public string OldName { get; set; }
        public string ForeignTable { get; set; }
        public string ForeignColumn { get; set; }
    }

    // Values are part of the wire format; never renumber them.
    public enum BDadosBackupDataType {
        Boolean = 1, Byte = 2, SByte = 3, Int16 = 4, UInt16 = 5, Int32 = 6, UInt32 = 7,
        Int64 = 8, UInt64 = 9, Single = 10, Double = 11, Decimal = 12, Char = 13,
        String = 14, Bytes = 15, Guid = 16, DateTime = 17, DateTimeOffset = 18,
        TimeSpan = 19, DateOnly = 20, TimeOnly = 21, BinarySerializable = 22
    }
}
