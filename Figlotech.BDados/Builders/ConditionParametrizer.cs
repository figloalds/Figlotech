
using Figlotech.BDados.Entity;
using Figlotech.Core;
/**
* Figlotech.BDados.Builders.ConditionParametrizer
* Extra Implementation for IQueryBuilder
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.Builders {
    public class ConditionParametrizer : QueryBuilder {
        int i;
        String RandomId {
            get {
                String s = IntEx.GerarShortRID();
                return $"{s}{i++}";
            }
        }
        public ConditionParametrizer(String fragment, params object[] args) {
            AppendCondition(fragment, args);
        }
        public ConditionParametrizer() {
        }

        public ConditionParametrizer AddStringEquality(String target, String inputValue) {
            if (inputValue != null)
                this.AppendCondition($"{target}=@{RandomId}", inputValue);
            return this;
        }

        public ConditionParametrizer AddNulityCondition(String target, bool? inputValue) {
            if(inputValue != null)
                this.AppendCondition($"({target} IS NULL)==@{RandomId}", inputValue);
            return this;
        }

        public ConditionParametrizer AddMaxDate(String target, String maxDateValue) {
            DateTime dt = DateTime.MaxValue;
            if (!DateTime.TryParseExact(maxDateValue, "yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dt)) {
                if (!DateTime.TryParseExact(maxDateValue, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dt)) {
                    return this;
                }
            }
            this.AppendCondition($"{target}<==@{RandomId}", dt);
            return this;
        }

        public ConditionParametrizer AddMinDate(String target, String minDateValue) {
            DateTime dt = DateTime.MaxValue;
            if (!DateTime.TryParseExact(minDateValue, "yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dt)) {
                if (!DateTime.TryParseExact(minDateValue, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dt)) {
                    return this;
                }
            }
            this.AppendCondition($"{target}<==@{RandomId}", dt);
            return this;
        }

        public ConditionParametrizer AddBooleanEquality(String target, bool? inputValue) {
            if (inputValue != null)
                this.AppendCondition($"{target}==@{RandomId}", inputValue);
            return this;
        }


        public ConditionParametrizer AppendCondition(String fragment, params object[] args) {
            if (!fragment.StartsWith(" "))
                fragment = $" {fragment}";
            if(!this.IsEmpty) {
                Append(" AND ");
            }
            Append(fragment, args);
            return this;
        }
    }
}
