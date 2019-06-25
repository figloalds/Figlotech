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
                query.ApplyToComand(command, Plugin);
                command.ExecuteNonQuery();
            }
        }

        public static void ApplyToComand(this IQueryBuilder query, IDbCommand command, IRdbmsPluginAdapter Plugin) {
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
                    paramRefl["Encoding"] = Fi.StandardEncoding;
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
