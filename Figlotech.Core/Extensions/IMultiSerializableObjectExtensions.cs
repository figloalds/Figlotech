using Figlotech.Core.Autokryptex;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Figlotech.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace Figlotech.Core.Extensions {

    public static class IMultiSerializableObjectExtensions {
        public static async Task ToJson(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;
            using (var ms = new MemoryStream()) {
                var json = JsonConvert.SerializeObject(obj, options.Formatted ? Newtonsoft.Json.Formatting.Indented : Newtonsoft.Json.Formatting.None);

                ms.Write(json);
                ms.Seek(0, SeekOrigin.Begin);

                var StreamOptions = GetSaveStreamOptions(options);
                await StreamOptions
                    .Process(ms, async (usableStream) => {
                        await Task.Yield();
                        await usableStream.CopyToAsync(rawStream);
                    });
            }
        }

        public static async Task ToJsonFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            //lock(obj) {
            await fs.Write(fileName, async fstream => {
                await Task.Yield();
                await obj.ToJson(fstream, options);
            });
            //}
        }

        public static async Task FromJson(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            var StreamOptions = GetOpenStreamOptions(options);

            await StreamOptions
                .Process(rawStream, async (usableStream) => {
                    using (var reader = new StreamReader(usableStream, Fi.StandardEncoding)) {
                        var json = await reader.ReadToEndAsync();
                        try {
                            var parse = JsonConvert.DeserializeObject(json, obj.GetType());
                            Fi.Tech.MemberwiseCopy(parse, obj);
                        } catch (Exception x) {
                            Fi.Tech.WriteLine("Error parsing JSON File: " + x.Message);
                            throw x;
                        }
                    }
                });
        }

        public static async Task FromJsonFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            //lock(obj) {
            await fs.Read(fileName, async fstream => {
                await Task.Yield();
                await obj.FromJson(fstream, options);
            });
            //}
        }

        private static IStreamProcessor GetSaveStreamOptions(FTHSerializableOptions options) {
            var retv = new BatchStreamProcessor();
            if (options?.UseGzip ?? false) {
                retv.Add(new GzipCompressStreamProcessor());
            }
            if (options?.UseEncryption != null) {
                retv.Add(new CypherStreamProcessor(options.UseEncryption));
            }
            return retv;
        }
        private static IStreamProcessor GetOpenStreamOptions(FTHSerializableOptions options) {
            var retv = new BatchStreamProcessor();
            if (options?.UseEncryption != null) {
                retv.Add(new DecypherStreamProcessor(options.UseEncryption));
            }
            if (options?.UseGzip ?? false) {
                retv.Add(new GzipDecompressStreamProcessor());
            }
            return retv;
        }

        public static async Task ToXml(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            var StreamOptions = GetSaveStreamOptions(options);

            using (var ms = new MemoryStream()) {

                XmlSerializer xsSubmit = new XmlSerializer(obj.GetType());
                var xml = "";

                using (var sww = new StringWriter()) {

                    using (XmlTextWriter writer = new XmlTextWriter(sww)) {
                        if (options.Formatted) {
                            writer.Formatting = System.Xml.Formatting.Indented;
                            writer.Indentation = 4;
                        }
                        xsSubmit.Serialize(writer, obj);
                        xml = sww.ToString();
                        using (var sw = new StreamWriter(ms, Fi.StandardEncoding)) {
                            sw.Write(xml);
                            ms.Seek(0, SeekOrigin.Begin);
                            await StreamOptions
                                .Process(ms, async (usableStream) => {
                                    await Task.Yield();
                                    await usableStream.CopyToAsync(rawStream);
                                });
                        }
                    }
                }
            }
        }

        public static async Task ToXmlFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            await fs.Write(fileName, async fstream => {
                await Task.Yield();
                await obj.ToXml(fstream, options);
            });
        }

        public static async Task FromXml(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            var StreamOptions = GetOpenStreamOptions(options);

            await StreamOptions
                .Process(rawStream, async (usableStream) => {
                    await Task.Yield();
                    var serializer = new XmlSerializer(obj.GetType());
                    // this is necessary because XML Deserializer is a bitch

                    using (StreamReader reader = new StreamReader(usableStream, Fi.StandardEncoding)) {
                        var retv = serializer.Deserialize(reader);
                        Fi.Tech.MemberwiseCopy(retv, obj);
                    }
                });
        }

        public static async Task FromXmlFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            await fs.Read(fileName, async fstream => {
                await obj.FromXml(fstream, options);
            });
        }
    }
}
