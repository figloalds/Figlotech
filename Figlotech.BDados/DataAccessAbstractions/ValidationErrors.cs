
using System.Collections.Generic;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class ValidationError {
        public string Key;
        public string Message;
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
    }
}
