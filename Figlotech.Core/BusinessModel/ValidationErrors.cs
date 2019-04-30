
using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core.BusinessModel {
    public class ValidationError {
        public string Key;
        public string Message;
    }

    public static class Validation {
        public static ValidationError Error(string msg) {
            return  Error("Error", msg);
        }

        public static ValidationError Error(string key, string msg) {
            return new ValidationError {
                Key = key,
                Message = msg
            };
        }
    }

    public class ValidationErrors : List<ValidationError> {
        public void Add(string key, string value) {
            Add(new ValidationError { Key = key, Message = value });
        }

        public void Merge(ValidationErrors other) {
            if (other == null)
                return;
            AddRange(other);

        }

        public override string ToString() {
            StringBuilder retv = new StringBuilder();
            foreach(var a in this) {
                retv.Append(a.Message);
                retv.Append("\n");
            }
            return retv.ToString();
        }

        public static ValidationErrors operator +(ValidationErrors verr, (string,string) operand) {
            verr.Add(new ValidationError() {
                Key = operand.Item1,
                Message = operand.Item2
            });
            return verr;
        }
    }
}
