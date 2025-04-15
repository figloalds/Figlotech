using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace System
{
    public sealed class ProgressEvent {
        public long Current { get; set; }
        public long Total { get; set; }

        public decimal Percentage => ((decimal)Current / (decimal)Total) * 100m;
    }

    public static class ObjectExtensions {
        public static void CopyFrom(this object me, object other) {
            if (me == null) {
                throw new NullReferenceException("Trying to copy into null");
            }

            Fi.Tech.MemberwiseCopy(other, me);
        }
        public static void CopyFromExcept(this object me, object other, params string[] except) {
            if (me == null) {
                throw new NullReferenceException("Trying to copy into null");
            }

            Fi.Tech.MemberwiseCopyExcept(other, me, except);
        }

        public static void CopyFromOnly(this object me, object other, params string[] only) {
            if (me == null) {
                throw new NullReferenceException("Trying to copy into null");
            }

            Fi.Tech.MemberwiseCopyOnly(other, me, only);
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

        public static Task<T> AsTask<T>(this T me) {
            return Task.FromResult(me);
        }

        public static void CopyFromAndMergeLists(this Object me, object other) {
            if (me == null) {
                throw new NullReferenceException("Figlotech CopyFrom Extension method called on a null value, this is a natural NullReferenceException");
            }

            if (other == null) {
                me = null;
                return;
            }
            var origin = other; // 'other' object to copy from
            var destination = me; // 'me' object to copy to

            var originType = origin.GetType();
            var destinationType = destination.GetType();
            var sameType = originType == destinationType;

            // Gather fields and properties of the origin object
            var originMembers = ReflectionTool.FieldsAndPropertiesOf(originType)
                .ToDictionary(member => member.Name);

            foreach (var destMember in ReflectionTool.FieldsAndPropertiesOf(destinationType)) {
                if (sameType || originMembers.ContainsKey(destMember.Name)) {
                    var originMember = sameType ? destMember : originMembers[destMember.Name];
                    var originValue = ReflectionTool.GetMemberValue(originMember, origin);

                    // Check if both fields/properties are of type List<>
                    if (ReflectionTool.IsListOfT(ReflectionTool.GetTypeOf(originMember)) && ReflectionTool.IsListOfT(ReflectionTool.GetTypeOf(originMember))) {
                        var destValue = ReflectionTool.GetMemberValue(destMember, destination);
                        MergeLists(originValue, destValue);
                    } else {
                        // Directly set the value if it's not a list or doesn't need special handling
                        ReflectionTool.SetMemberValue(destMember, destination, originValue);
                    }
                }
            }
        }

        static void MergeLists(object originList, object destinationList) {
            var elementType = ReflectionTool.GetListElementType(originList.GetType());
            var addMethod = destinationList.GetType().GetMethod("Add");
            var containsMethod = destinationList.GetType().GetMethod("Contains");
            var listsHaveSameType = elementType == ReflectionTool.GetListElementType(destinationList.GetType());
            var originListElementTypeIsDataObject = ReflectionTool.GetListElementType(originList).Implements(typeof(IDataObject));
            var destinationListElementTypeIsDataObject = listsHaveSameType || ReflectionTool.GetListElementType(destinationList).Implements(typeof(IDataObject));

            foreach (var item in (IEnumerable)originList) {
                // Handle the merging of complex elements if necessary, e.g., if the lists contain objects that themselves have properties to merge

                if(originListElementTypeIsDataObject && listsHaveSameType) {
                    var existingItem = FindDataObjectInList(destinationList, item);
                    if (existingItem != null) {
                        // Optionally handle deep merging here
                        CopyFromAndMergeLists((IDataObject) item, existingItem);
                    } else {
                        addMethod.Invoke(destinationList, new[] { item });
                    }
                } else {
                    if (!(bool)containsMethod.Invoke(destinationList, new[] { item })) {
                        addMethod.Invoke(destinationList, new[] { item });
                    }
                }
            }
        }
        static object FindDataObjectInList(object list, object item) {
            foreach(var i in ReflectionTool.EnumerateList(list)) {
                if(((IDataObject) i).RID == ((IDataObject) item).RID) {
                    return i;
                }
            }
            return null;
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

            foreach(var data in meta.Item2) {
                dr[data.ColumnName] = ReflectionTool.GetValue(me, data.ColumnName);
            }
        }

        public static List<T> ToSingleElementList<T>(this T me) {
            return new List<T> { me };
        }
        public static object ToSingleElementListRefl(this object me) {
            var t = me.GetType();
            var li = typeof(List<>).MakeGenericType(t);
            var retv = Activator.CreateInstance(li);
            li.GetMethod(nameof(List<object>.Add), new Type[] { t }).Invoke(retv, new object[] { me });
            return retv;
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
            foreach (var a in ReflectionTool.FieldsAndPropertiesOf(me.GetType())) {
                if (ReflectionTool.GetTypeOf(a).IsPublic && input.ContainsKey(a.Name)) {
                    ReflectionTool.SetValue(me, a.Name, input[a.Name]);
                }
            }
        }

        public static object ReflGet(this Object me, string prop) {
            if(me == null) {
                return null;
            }
            return ReflectionTool.GetValue(me, prop);
        }
        public static object ReflGet(this Object me, MemberInfo prop) {
            if (me == null) {
                return null;
            }
            return ReflectionTool.GetMemberValue(prop, me);
        }
        public static void ReflSet(this Object me, string prop, object value) {
            if (me == null) {
                return;
            }
            ReflectionTool.SetValue(me, prop, value);
        }
        public static void ReflSet(this Object me, MemberInfo prop, object value) {
            if (me == null) {
                return;
            }
            ReflectionTool.SetMemberValue(prop, me, value);
        }
        public static IEnumerable<(MemberInfo Key, object Value)> ReflEnumeratePropsAndValues(this Object me) {
            if(me == null) {
                yield break;
            }
            foreach(var item in ReflectionTool.FieldsAndPropertiesOf(me.GetType())) {
                yield return (item, ReflectionTool.GetMemberValue(item, me));
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
