using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace Figlotech.BDados.Helpers {
    public class FTHSerializableOptions {
        public bool UseGzip { get; set; }
    }

    public static class IMultiSerializableObjectExtensions {
        public static void ToJson(this IMultiSerializableObject obj, Stream ms, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            if (options?.UseGzip ?? false) {
                ms = new GZipStream(ms, CompressionLevel.Optimal);
            }
            var json = JsonConvert.SerializeObject(obj);
            using (var writter = new StreamWriter(ms, Encoding.UTF8)) {
                writter.Write(json);
            }
        }

        public static void ToJsonFile(this IMultiSerializableObject obj, String fileName, FTHSerializableOptions options = null) {
            using (var fs = File.Open(fileName, FileMode.OpenOrCreate)) {
                obj.ToJson(fs, options);
            }
        }

        public static void FromJson(this IMultiSerializableObject obj, Stream stream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            if (options?.UseGzip ?? false) {
                stream = new GZipStream(stream, CompressionMode.Decompress);
            }
            using (var reader = new StreamReader(stream, Encoding.UTF8)) {
                var json = reader.ReadToEnd();
                try {
                    var parse = JsonConvert.DeserializeObject(json, obj.GetType());
                    FTH.MemberwiseCopy(parse, obj);
                } catch (Exception x) {
                    FTH.WriteLine("Error parsing JSON File: " + x.Message);
                    throw x;
                }
            }
        }

        public static void FromJsonFile(this IMultiSerializableObject obj, String fileName, FTHSerializableOptions options = null) {
            obj.FromJson(File.Open(fileName, FileMode.Open), options);
        }


        public static void ToXml(this IMultiSerializableObject obj, Stream ms, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            if (options?.UseGzip ?? false) {
                ms = new GZipStream(ms, CompressionLevel.Optimal);
            }

            XmlSerializer xsSubmit = new XmlSerializer(obj.GetType());
            var xml = "";

            using (var sww = new StringWriter()) {
                using (XmlWriter writer = XmlWriter.Create(sww)) {
                    xsSubmit.Serialize(writer, obj);
                    xml = sww.ToString();
                    using (var sw = new StreamWriter(ms)) {
                        sw.Write(xml);
                    }
                }
            }
        }

        public static void ToXmlFile(this IMultiSerializableObject obj, String fileName, FTHSerializableOptions options = null) {
            using (var fs = File.Open(fileName, FileMode.OpenOrCreate)) {
                obj.ToXml(fs, options);
            }
        }

        public static void FromXml(this IMultiSerializableObject obj, Stream stream, FTHSerializableOptions options = null) {
            if (obj == null)
                return;

            if (options?.UseGzip ?? false) {
                stream = new GZipStream(stream, CompressionMode.Decompress);
            }

            var serializer = new XmlSerializer(obj.GetType());
            var retv = serializer.Deserialize(stream);

            FTH.MemberwiseCopy(retv, obj);
        }

        public static void FromXmlFile(this IMultiSerializableObject obj, String fileName, FTHSerializableOptions options = null) {
            obj.FromXml(File.Open(fileName, FileMode.Open), options);
        }
    }
}
