using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Numerics;
using System.Text;

namespace Figlotech.Data
{

    public static class IQueryBuilderExtensions {

        private static bool IsSupportedParameterType(Type type) {
            if (type == null) {
                return false;
            }

            if (type.IsEnum || typeof(Stream).IsAssignableFrom(type)) {
                return true;
            }

            if (type == typeof(bool)
                || type == typeof(byte)
                || type == typeof(char)
                || type == typeof(double)
                || type == typeof(float)
                || type == typeof(int)
                || type == typeof(long)
                || type == typeof(sbyte)
                || type == typeof(short)
                || type == typeof(uint)
                || type == typeof(ulong)
                || type == typeof(ushort)
                || type == typeof(BigInteger)
                || type == typeof(DateTime)
                || type == typeof(DateTimeOffset)
                || type == typeof(decimal)
                || type == typeof(Guid)
                || type == typeof(string)
                || type == typeof(TimeSpan)
                || type == typeof(byte[])
                || type == typeof(float[])
                || type == typeof(StringBuilder)
                || type == typeof(DBNull)
                || type.FullName == "System.DateOnly"
                || type.FullName == "System.TimeOnly") {
                return true;
            }

            if (type.IsGenericType) {
                var genericType = type.GetGenericTypeDefinition();
                var genericTypeArg = type.GetGenericArguments()[0];
                if ((genericType == typeof(ArraySegment<>) || genericType == typeof(Memory<>) || genericType == typeof(ReadOnlyMemory<>))
                    && (genericTypeArg == typeof(byte) || genericTypeArg == typeof(float) || genericTypeArg == typeof(char))) {
                    return true;
                }
            }

            return false;
        }

        public static IQueryBuilder ToQueryBuilder(this string query, params object[] args) {
            return Qb.Fmt(query, args);
        }
        public static IQueryBuilder ToQueryBuilder(this FormattableString query) {
            return Qb.S(query);
        }

        public static void ApplyToCommand(this IQueryBuilder query, IDbCommand command, Func<object, object> ProcessParameterValue = null) {
            String QueryText = query.GetCommandText();
            command.CommandText = QueryText;
            // Adiciona os parametros
            foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                var cmdParam = command.CreateParameter();
                cmdParam.ParameterName = param.Key;

                var usableValue = param.Value;
                if (ProcessParameterValue != null) {
                    usableValue = ProcessParameterValue.Invoke(param.Value);
                }
                if (usableValue != null && usableValue.GetType().DerivesFromGeneric(typeof(ValueBox<>))) {
                    usableValue = usableValue.GetType().GetProperty("Value").GetValue(usableValue);
                }
                if (usableValue == null) {
                    cmdParam.Value = DBNull.Value;
                } else if (usableValue is String str) {
                    cmdParam.Value = str;
                    cmdParam.DbType = DbType.String;
                } else if (usableValue.GetType().IsEnum) {
                    cmdParam.Value = (int)usableValue;
                } else if (IsSupportedParameterType(usableValue.GetType())) {
                    cmdParam.Value = usableValue;
                } else {
                    cmdParam.Value = usableValue.ToString() ?? String.Empty;
                    cmdParam.DbType = DbType.String;
                }
                cmdParam.Direction = ParameterDirection.Input;

                command.Parameters.Add(cmdParam);
            }
        }
    }

    public interface IQueryBuilder {

        IQueryBuilder Append(string Text, params object[] args);
        IQueryBuilder Append(IQueryBuilder other);

        Dictionary<String, Object> GetParameters();
        string GetCommandText();

        long Id { get; }
        bool IsEmpty { get; }
    }
}
