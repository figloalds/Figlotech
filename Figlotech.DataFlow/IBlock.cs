using System;
using System.IO;
using System.Threading.Tasks;

namespace Figlotech.DataFlow {

    public interface IBlock {
        ValueTask Execute();
    }

    public class Input : Attribute { 
        public string Description { get; set; }
        public bool Optional { get; set; }

        public Input(string description, bool optional = false) {
            Description = description;
            Optional = optional;
        }
    }
    public class Output : Attribute {
        public string Description { get; set; }
        public Output(string description) {
            Description = description;
        }
    }

    public class Block : Attribute {
        public string Description { get; set; }
    }

    public class StringConstantBlock : IBlock {
        [Input("Value of the string constant", optional: true)]
        public string Value { get; set; }

        [Output("String")]
        public ValueTask<string> GetValue() {
            return new ValueTask<string>(Value);
        }

        public ValueTask Execute() {
            return new ValueTask();
        }
    }

    public class HttpRequest : IBlock {
        [Input("URL to request")]
        public string Url { get; set; }

        public async ValueTask Execute() {
            using(var httpRequest = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Get, Url)) {
                using(var httpClient = new System.Net.Http.HttpClient()) {
                    var response = await httpClient.SendAsync(httpRequest).ConfigureAwait(false);
                    StatusCode = (int)response.StatusCode;
                    ResponseBody = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                }
            }
        }

        [Output("Status Code")]
        public int StatusCode { get; set; } = 0;

        [Output("Response Body")]
        public Stream ResponseBody { get; set; }

    }
}
