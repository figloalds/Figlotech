using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace System
{
    public class ProgressEvent {
        public long Current { get; set; }
        public long Total { get; set; }

        public decimal Percentage => ((decimal)Current / (decimal)Total) * 100m;
    }

    public static class ObjectExtensions {
        public static void CopyFrom(this object me, object other) {
            if (me == null) {
                throw new NullReferenceException("Trying to copy into null");
            }
            if (other == null) {
                throw new NullReferenceException("Trying to copy a null value");
            }
            Fi.Tech.MemberwiseCopy(other, me);
        }

        public static async Task CopyFile(this IFileSystem fs, string relative, IFileSystem other, string relative2, Action<ProgressEvent> onProgress = null) {
            await fs.Read(relative, async input => {
                await Task.Yield();
                await other.Write(relative2, async output => {
                    await Task.Yield();
                    int bufferSize = 81920;
                    byte[] buffer = new byte[bufferSize];
                    int read;
                    var pe = new ProgressEvent();
                    pe.Total = input.Length;
                    while ((read = await input.ReadAsync(buffer, 0, buffer.Length)) != 0) {
                        await output.WriteAsync(buffer, 0, read);
                        pe.Current += read;
                        onProgress?.Invoke(pe);
                    }
                });
            });
        }

        public static void CopyFromAndMergeLists(this Object me, object other) {
            if (me == null) {
                throw new NullReferenceException("Figlotech CopyFrom Extension method called on a null value, this is a natural NullReferenceException");
            }

            if (other == null) {
                me = null;
                return;
            }
            ObjectReflector.Open(other, (objA) => {
                ObjectReflector.Open(me, (objB) => {
                    foreach (var field in objB) {
                        if (objA.ContainsKey(field.Key.Name)) {
                            var valA = objA[field.Key.Name];
                            var valB = objB[field.Key.Name];
                            if (
                                valA.GetType().IsGenericType && valA.GetType().Implements(typeof(List<>)) &&
                                valB.GetType().IsGenericType && valB.GetType().Implements(typeof(List<>))
                            ) {
                                var addMethod = valB.GetType().GetMethods().FirstOrDefault(m => m.Name == "Add" && m.GetParameters().Length == 1);
                                var enny = (IEnumerator) valA.GetType().GetMethods().FirstOrDefault(e => e.Name == "GetEnumerator")?.Invoke(valA, new object[0]);
                                var fodefMethod = valB.GetType().GetMethods().FirstOrDefault(m => m.Name == "FirstOrDefault" && m.GetParameters().Length == 1);
                                while (enny.MoveNext()) {
                                    var paramEx = Expression.Parameter(valB.GetType().GetGenericArguments().First(), "a");
                                    var lambda = Expression.Lambda(Expression.Equal(paramEx, Expression.Constant(enny.Current)), paramEx);
                                    var fodef = fodefMethod?.Invoke(valB, new object[] { lambda });
                                    if (fodef != null) {
                                        if(!fodef.Equals(enny.Current)) {
                                            addMethod.Invoke(valB, new object[] { enny.Current });
                                        } else {
                                            CopyFromAndMergeLists(fodef, enny.Current);
                                        }
                                    }
                                }
                            }
                            objB[field.Key] = objA[field.Key.Name];
                        }
                    }
                });
            });
        }

        public static T InvokeGenericMethod<T>(this Object me, string GenericMethodName, Type genericType, params object[] args) {
            return (T)InvokeGenericMethod(me, GenericMethodName, genericType, args);
        }
        public static object InvokeGenericMethod(this Object me, string GenericMethodName, Type genericType, params object[] args) {
            var method = me.GetType()
                .GetMethods().FirstOrDefault(
                    m => m.Name == (GenericMethodName) &&
                    m.GetParameters().Length == args.Length &&
                    m.GetGenericArguments().Length == 1
                    );
            if(method == null) {
                throw new MissingMethodException($"Generic method not found: {me.GetType()}::{GenericMethodName}<{genericType.Name}>({string.Join(",", args.Select(a=> a?.GetType()?.Name))})");
            }
            return method.MakeGenericMethod(genericType)
                .Invoke(me, args);
        }

        public static void ValuesFromDataRow(this Object me, DataRow dr, Tuple<List<MemberInfo>, List<DataColumn>> meta = null) {
            var type = me.GetType();
            if (meta == null)
                meta = Fi.Tech.Invoke<Tuple<List<MemberInfo>, List<DataColumn>>>(
                    typeof(FiTechCoreExtensions),
                    nameof(FiTechCoreExtensions.MapMeta),
                    type,
                    dr);
            Fi.Tech.Map(me, dr, meta);
        }

        public static void ValuesToDataRow(this Object me, DataRow dr, Tuple<List<MemberInfo>, List<DataColumn>> meta = null) {
            var type = me.GetType();
            if (meta == null)
                meta = Fi.Tech.Invoke<Tuple<List<MemberInfo>, List<DataColumn>>>(
                    typeof(FiTechCoreExtensions),
                    nameof(FiTechCoreExtensions.MapMeta),
                    type,
                    dr);
            var refl = me.AsReflectable();
            foreach(var data in meta.Item2) {
                dr[data.ColumnName] = refl[data.ColumnName];
            }
        }

        public static ObjectReflector AsReflectable(this Object me) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToReflectable Extension method called on a null value, this is a natural NullReferenceException");
            }
            return new ObjectReflector(me);
        }

        public static List<T> ToSingleElementList<T>(this T me) {
            return new List<T> { me };
        }

        public static Any<T> ToAny<T>(this T me) {
            return new Any<T>(me);
        }

        public static T ValueTo<T>(this Object me) {
            try {
                return (T)Convert.ChangeType(me, typeof(T));
            } catch (Exception) {
                return default(T);
            }
        }

        public static Dictionary<string, object> ValuesToDictionary(this Object me) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }

            var retv = new Dictionary<string, object>();
            var refl = me.AsReflectable();
            foreach(var a in ReflectionTool.FieldsAndPropertiesOf(me.GetType())) {
                if(ReflectionTool.GetTypeOf(a).IsPublic) {
                    retv[a.Name] = ReflectionTool.GetMemberValue(a, me);
                }
            }
            return retv;
        }

        public static void ValuesFromDictionary(this Object me, Dictionary<string, object> input) {
            if (me == null) {
                throw new NullReferenceException("Figlotech FromDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }
            var refl = me.AsReflectable();
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
            return JsonConvert.SerializeObject(me, new JsonSerializerSettings {
                Formatting = formatted ? Formatting.Indented : Formatting.None,
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                DateFormatHandling = DateFormatHandling.IsoDateFormat,
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
            });
        }
        public static byte[] ToJsonb(this Object me, bool formatted = false) {
            return Fi.StandardEncoding.GetBytes(ToJson(me, formatted));
        }
        public static async Task WriteObjectAsJsonAsync(this Stream me, object obj) {
            var bytes = obj.ToJsonb();
            await me.WriteAsync(bytes, 0, bytes.Length);
        }

        public static void FromJson(this Object me, string json) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }
            Fi.Tech.MemberwiseCopy(JsonConvert.DeserializeObject(json, me.GetType()), me);
        }
        public static object ToObjectFromJsonB<T>(this byte[] me) {
            if (me == null) {
                throw new NullReferenceException("Figlotech ToDictionary Extension method called on a null value, this is a natural NullReferenceException");
            }
            return JsonConvert.DeserializeObject<T>(Fi.StandardEncoding.GetString(me));
        }
    }
}
