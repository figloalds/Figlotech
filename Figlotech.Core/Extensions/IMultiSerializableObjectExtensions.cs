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
        public static void ToJson(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;
            using (var ms = new MemoryStream()) {
                var json = JsonConvert.SerializeObject(obj, options.Formatted ? Newtonsoft.Json.Formatting.Indented : Newtonsoft.Json.Formatting.None);

                ms.Write(json);
                ms.Seek(0, SeekOrigin.Begin);

                var StreamOptions = GetSaveStreamOptions(options);
                StreamOptions
                    .Process(ms, (usableStream) => {
                        usableStream.CopyTo(rawStream);
                    });
            }
        }

        public static void ToJsonFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            //lock(obj) {
            fs.Write(fileName, fstream => {
                obj.ToJson(fstream, options);
            });
            //}
        }

        public static void FromJson(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            var StreamOptions = GetOpenStreamOptions(options);

            StreamOptions
                .Process(rawStream, (usableStream) => {
                    using (var reader = new StreamReader(usableStream, Fi.StandardEncoding)) {
                        var json = reader.ReadToEnd();
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

        public static void FromJsonFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            //lock(obj) {
            fs.Read(fileName, fstream => {
                obj.FromJson(fstream, options);
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

        public static void ToXml(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
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
                            StreamOptions
                                .Process(ms, (usableStream) => {
                                    usableStream.CopyTo(rawStream);
                                });
                        }
                    }
                }
            }
        }

        public static void ToXmlFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            fs.Write(fileName, fstream => {
                obj.ToXml(fstream, options);
            });
        }

        public static void FromXml(this IMultiSerializableObject obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            var StreamOptions = GetOpenStreamOptions(options);

            StreamOptions
                .Process(rawStream, (usableStream) => {

                    var serializer = new XmlSerializer(obj.GetType());
                    // this is necessary because XML Deserializer is a bitch

                    using (StreamReader reader = new StreamReader(usableStream, Fi.StandardEncoding)) {
                        var retv = serializer.Deserialize(reader);
                        Fi.Tech.MemberwiseCopy(retv, obj);
                    }
                });
        }

        public static void FromXmlFile(this IMultiSerializableObject obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            fs.Read(fileName, fstream => {
                obj.FromXml(fstream, options);
            });
        }
    }
}
