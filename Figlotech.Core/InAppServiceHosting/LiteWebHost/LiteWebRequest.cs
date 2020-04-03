using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Figlotech.Core.InAppServiceHosting.LiteWebHost
{
    public class LiteWebRequest
    {
        public string RequestUri { get; set; }
        public Dictionary<string,string> Query { get; set; }
        public Dictionary<string,string> RequestHeaders { get; set; }
        public byte[] RequestBody { get; set; }
        public Stream StreamForResponse { get; set; }
        public LiteWebRequest(string reqUri, IDictionary<string, string> query, Dictionary<string, string> reqHeaders, byte[] bodyBytes, Stream stream) {

        }
    }
}
