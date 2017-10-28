using Figlotech.Core;
using Figlotech.Core.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace System
{
    public static class ObjectExtensions
    {
        public static void CopyFrom(this Object me, object other) {
            if (me == null) {
                throw new NullReferenceException("Figlotech CopyFrom Extension method called on a null value, this is a natural NullReferenceException");
            }
            if (other == null) {
                me = null;
                return;
            }

            Fi.Tech.MemberwiseCopy(other, me);
        }


        public static T Invoke<T>(this Object me, string GenericMethodName, Type genericType, params object[] args) {
            return (T) me.GetType()
                .GetMethod(nameof(FiTechCoreExtensions.MapMeta))
                .MakeGenericMethod(genericType)
                .Invoke(null, args);
        }

        public static void FromDataRow(this Object me, DataRow dr, Tuple<List<MemberInfo>, List<DataColumn>> meta = null) {
            var type = me.GetType();
            if (meta == null)
                meta = Fi.Tech.Invoke<Tuple<List<MemberInfo>, List<DataColumn>>>(
                    typeof(FiTechCoreExtensions),
                    nameof(FiTechCoreExtensions.MapMeta),
                    type,
                    dr);
            Fi.Tech.Map(me, dr, meta);
        }
        public static void ToDataRow(this Object me, DataRow dr, Tuple<List<MemberInfo>, List<DataColumn>> meta = null) {
            var type = me.GetType();
            if (meta == null)
                meta = Fi.Tech.Invoke<Tuple<List<MemberInfo>, List<DataColumn>>>(
                    typeof(FiTechCoreExtensions),
                    nameof(FiTechCoreExtensions.MapMeta),
                    type,
                    dr);
            var refl = me.ToReflectable();
            foreach(var data in meta.Item2) {
                dr[data.ColumnName] = refl[data.ColumnName];
            }
        }

        public static ObjectReflector ToReflectable(this Object me) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToReflectable Extension method called on a null value, this is a natural NullReferenceException");
            }
            return new ObjectReflector(me);
        }

        public static Dictionary<string, object> ToDictionary(this Object me) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }

            var retv = new Dictionary<string, object>();
            var refl = me.ToReflectable();
            foreach(var a in ReflectionTool.FieldsAndPropertiesOf(me.GetType())) {
                if(ReflectionTool.GetTypeOf(a).IsPublic) {
                    retv[a.Name] = ReflectionTool.GetMemberValue(a, me);
                }
            }
            return retv;
        }

        public static void FromDictionary(this Object me, Dictionary<string, object> input) {
            if (me == null) {
                throw new NullReferenceException("Figlotech FromDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }
            var refl = me.ToReflectable();
            foreach (var a in ReflectionTool.FieldsAndPropertiesOf(me.GetType())) {
                if (ReflectionTool.GetTypeOf(a).IsPublic && input.ContainsKey(a.Name)) {
                    refl[a.Name] = input[a.Name];
                }
            }
        }

        public static string ToJson(this Object me, bool formatted = false) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }
            return JsonConvert.SerializeObject(me, formatted ? Formatting.Indented : Formatting.None);
        }

        public static void FromJson(this Object me, string json) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }
            Fi.Tech.MemberwiseCopy(JsonConvert.DeserializeObject(json, me.GetType()), me);
        }
    }
}
