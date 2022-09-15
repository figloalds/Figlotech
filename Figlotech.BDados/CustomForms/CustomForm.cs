﻿using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.BDados.CustomForms {
    public sealed class CustomForm {
        public String Name;
        public String Description;
        public String Module;
        public String Component;
        public List<CustomFormField> Fields;

        public CustomForm() { }

        public static CustomForm LoadFromFile(IFileSystem fa, String Relative) {
            if (fa == null)
                return null;
            try {
                String RawJson = Fi.StandardEncoding.GetString(fa.ReadAllBytes(Relative));
                if (RawJson == null || RawJson.Length == 0) {
                    throw new CustomFormsException("Custom Form not found.");
                }

                CustomForm c = JsonConvert.DeserializeObject<CustomForm>(RawJson);

                return c;
            } catch(Exception x) {
                throw new CustomFormsException($"Custom Form was in an invalid format: {x.Message}");
            }

        }
    }
}
