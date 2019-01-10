using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {

    public static class IQueryBuilderExtensions {

        public static void ExecuteUsing(this IQueryBuilder query, IDbConnection conn) {
            using (var command = conn.CreateCommand()) {
                query.ApplyToComand(command);
                command.ExecuteNonQuery();
            }
        }

        public static void ApplyToComand(this IQueryBuilder query, IDbCommand command) {
            String QueryText = query.GetCommandText();
            command.CommandText = QueryText;
            // Adiciona os parametros
            var paramRefl = new ObjectReflector();
            foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                var cmdParam = command.CreateParameter();
                cmdParam.ParameterName = param.Key;
                if (param.Value is String str) {
                    cmdParam.Value = str;
                    cmdParam.DbType = DbType.String;
                    paramRefl.Slot(cmdParam);
                    paramRefl["Encoding"] = Encoding.UTF8;
                } else {
                    cmdParam.Value = param.Value;
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

        IQueryBuilder If(bool condition);
        IQueryBuilder Then();
        IQueryBuilder EndIf();
        IQueryBuilder Else();
    }
}
