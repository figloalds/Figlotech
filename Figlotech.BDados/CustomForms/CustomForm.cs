using Figlotech.BDados.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.CustomForms {
    public class CustomForm {
        public String Name;
        public String Description;
        public String Module;
        public String Component;
        public List<CustomFormField> Fields;

        public CustomForm() { }

        public static CustomForm LoadFromFile(IFileAccessor fa, String Relative) {
            if (fa == null)
                return null;
            try {
                String RawJson = Encoding.UTF8.GetString(fa.ReadAllBytes(Relative));
                if (RawJson == null || RawJson.Length == 0) {
                    throw new CustomFormsException("Custom Form not found.");
                }

                CustomForm c = JsonConvert.DeserializeObject<CustomForm>(RawJson);

                return c;
            } catch(Exception x) {
                throw new CustomFormsException($"Custom Form was in an invalid format: {x.Message}");
            }
            return null;
        }
    }
}
