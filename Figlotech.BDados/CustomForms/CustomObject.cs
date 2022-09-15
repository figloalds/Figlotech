﻿using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core.Interfaces;
using Figlotech.BDados;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using Figlotech.Core;

namespace Figlotech.BDados.CustomForms {
    public sealed class CustomObject
    {

        IRdbmsDataAccessor DataAccessor;

        private IDictionary<String, Object> Object = new Dictionary<String, Object>();
        public CustomObject() {
            Set("RID", IntEx.GenerateUniqueRID());
        }
        public CustomObject(Object input) {
            if(input is JObject) {
                Object = (input as JObject).ToObject<Dictionary<String, Object>>();
            } else {
                var Fields = input.GetType().GetFields();
                foreach (var a in Fields) {
                    Set(a.Name, a.GetValue(input));
                }
            }
            if(Get("RID") == null) {
                Set("RID", IntEx.GenerateUniqueRID());
            }
        }

        public Object Get(String Key) {
            if (Object.ContainsKey(Key)) {
                try {
                    return Object[Key];
                }
                catch (Exception) {

                }
            }
            return null;
        }

        public Object Refine() {
            return (Object) Object;
        }

        public void Set(String Key, Object Value) {
            if (Object.ContainsKey(Key)) {
                Object.Remove(Key);
            }
            Object.Add(Key, Value);
        }
        
    }
}
