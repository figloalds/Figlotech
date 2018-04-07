using Figlotech.Core.Autokryptex;
using Figlotech.Core.Extensions;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Serialization;

namespace Figlotech.Core.Helpers
{

    public static class SerializationUtil {
        public static void ToJson(object obj, Stream rawStream, FTHSerializableOptions options = null) {
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

        public static void ToJsonFile(object obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            //lock(obj) {
            fs.Write(fileName, fstream => {
                ToJson(obj, fstream, options);
            });
            //}
        }

        public static object FromJson<T>(Stream rawStream, FTHSerializableOptions options = null) {
            return (T)FromJson(typeof(T), rawStream, options);
        }
        public static object FromJson(Type t, Stream rawStream, FTHSerializableOptions options = null) {
            
            var StreamOptions = GetOpenStreamOptions(options);
            object retv = null;
            StreamOptions
                .Process(rawStream, (usableStream) => {

                    using (var reader = new StreamReader(usableStream, Encoding.UTF8)) {
                        var json = reader.ReadToEnd();
                        try {
                            var parse = JsonConvert.DeserializeObject(json, t);
                            retv = parse;
                        } catch (Exception x) {
                            Fi.Tech.WriteLine("Error parsing JSON File: " + x.Message);
                            throw x;
                        }
                    }
                });

            return retv;
        }

        public static T FromJsonFile<T>(IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            return (T)FromJsonFile(typeof(T), fs, fileName, options);
        }
        public static object FromJsonFile(Type t, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            //lock(obj) {
            object retv = null;
            fs.Read(fileName, fstream => {
                retv = FromJson(t, fstream, options);
            });
            return retv;
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

        public static void ToXml(object obj, Stream rawStream, FTHSerializableOptions options = null) {
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
                        using (var sw = new StreamWriter(ms, Encoding.UTF8)) {
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

        public static void ToXmlFile(object obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            fs.Write(fileName, fstream => {
                ToXml(obj, fstream, options);
            });
        }

        public static void FromXml(object obj, Stream rawStream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            var StreamOptions = GetOpenStreamOptions(options);

            StreamOptions
                .Process(rawStream, (usableStream) => {

                    var serializer = new XmlSerializer(obj.GetType());
                    // this is necessary because XML Deserializer is a bitch

                    using (StreamReader reader = new StreamReader(usableStream, Encoding.UTF8)) {
                        var retv = serializer.Deserialize(reader);
                        Fi.Tech.MemberwiseCopy(retv, obj);
                    }
                });
        }

        public static void FromXmlFile(object obj, IFileSystem fs, String fileName, FTHSerializableOptions options = null) {
            fs.Read(fileName, fstream => {
                FromXml(obj, fstream, options);
            });
        }
    }
}
