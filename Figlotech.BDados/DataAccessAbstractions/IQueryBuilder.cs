using Figlotech.BDados.Builders;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {

    public static class IQueryBuilderExtensions {

        public static void ExecuteUsing(this IQueryBuilder query, IDbConnection conn, IRdbmsPluginAdapter Plugin) {
            using (var command = conn.CreateCommand()) {
                query.ApplyToCommand(command, Plugin);
                command.ExecuteNonQuery();
            }
        }

        public static IQueryBuilder ToQueryBuilder(this string query) {
            return Qb.Fmt(query);
        }
        public static IQueryBuilder ToAutoQuery(this FormattableString query) {
            return Qb.S(query);
        }

        public static void ApplyToCommand(this IQueryBuilder query, IDbCommand command, Func<object, object> ProcessParameterValue = null) {
            String QueryText = query.GetCommandText();
            command.CommandText = QueryText;
            // Adiciona os parametros
            var paramRefl = new ObjectReflector();
            foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                var cmdParam = command.CreateParameter();
                cmdParam.ParameterName = param.Key;

                var usableValue = param.Value;
                if (ProcessParameterValue != null) {
                    usableValue = ProcessParameterValue.Invoke(param.Value);
                }
                if (usableValue == null) {
                    cmdParam.Value = DBNull.Value;
                } else
                if (usableValue is String str) {
                    cmdParam.Value = str;
                    cmdParam.DbType = DbType.String;
                    paramRefl.Slot(cmdParam);
                    //paramRefl["Encoding"] = Fi.StandardEncoding;
                } else {
                    cmdParam.Value = usableValue;
                }
                cmdParam.Direction = ParameterDirection.Input;

                command.Parameters.Add(cmdParam);
            }
        }
        public static void ApplyToCommand(this IQueryBuilder query, IDbCommand command, IRdbmsPluginAdapter Plugin) {
            String QueryText = query.GetCommandText();
            command.CommandText = QueryText;
            // Adiciona os parametros
            var paramRefl = new ObjectReflector();
            foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                var cmdParam = command.CreateParameter();
                cmdParam.ParameterName = param.Key;
                var usableValue = Plugin.ProcessParameterValue(param.Value);
                if(usableValue == null) {
                    cmdParam.Value = DBNull.Value;
                } else
                if (usableValue is String str) {
                    cmdParam.Value = str;
                    cmdParam.DbType = DbType.String;
                    paramRefl.Slot(cmdParam);
                    //paramRefl["Encoding"] = Fi.StandardEncoding;
                } else {
                    cmdParam.Value = usableValue;
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

        bool IsEmpty { get; }
        int Id { get; }

        IQueryBuilder If(bool condition);
        IQueryBuilder Then();
        IQueryBuilder EndIf();
        IQueryBuilder Else();
    }
}
