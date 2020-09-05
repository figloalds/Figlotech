using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Data
{
    public class SimpleCSVFormatter
    {
        char Sep { get; set; }
        public SimpleCSVFormatter(char sep = ';') {
            this.Sep = sep;
        }
        
        public async Task WriteObjectsToCsv<T>(IEnumerable<T> objs, Stream stream, bool printHeaders = true) {
            var headers = ReflectionTool.FieldsAndPropertiesOf(typeof(T)).Select(x=> x.Name).ToArray();
            if(printHeaders) {
                WriteHeadersToStream(headers, stream);
            }
            var li = objs.ToList();
            await Fi.Tech.ParallelFlow<T>((ch) => {
                ch.ReturnRange(li);
            }).Then(Math.Max(Environment.ProcessorCount-1, 1), async (c) => {
                return FetchObjectData(headers, new ObjectReflector(c));
            }).Then(1, async (data) => {
                WriteDataToStream(data, stream);
            });
        }

        public void WriteHeadersToStream(string[] headers, Stream stream) {
            var data = Encoding.UTF8.GetBytes(string.Join($"{Sep}", headers) + "\r\n");
            stream.Write(data, 0, data.Length);
        }

        public byte[] FetchObjectData(string[] headers, IReadOnlyDictionary<string, object> obj) {
            var data = Encoding.UTF8.GetBytes(string.Join($"{Sep}", headers.Select(x => {
                if(obj is ObjectReflector || obj.ContainsKey(x)) {
                    if(obj[x] == null) {
                        return "";
                    } else if (obj[x] is string s) {
                        if(s.Contains(Sep)) {
                            return $"\"{s}\"";
                        } else {
                            return s;
                        }
                    } else if (obj[x].GetType().IsClass) {
                        return "";
                    } else {
                        return obj[x].ToString();
                    }
                } else {
                    return "";
                }
            })) + "\r\n");
            return data;
        }
        public void WriteDataToStream(byte[] data, Stream stream) {
            stream.Write(data, 0, data.Length);
        }
    }
}
