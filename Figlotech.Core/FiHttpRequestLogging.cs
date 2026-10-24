using Figlotech.Core.FileAcessAbstractions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.Core {

    /// <summary>
    /// Writes one JSON file per HTTP exchange (request + response) so third-party support can be given the full
    /// conversation. Secrets are masked before writing: values of <see cref="RedactedNames"/> (headers, JSON
    /// properties at any depth, form fields and query parameters) and any occurrence of <see cref="RedactedValues"/>.
    /// </summary>
    public sealed class FiHttpRequestLogging {
        public const string RedactedPlaceholder = "****";

        /// <summary>Names masked by default; Authorization-like headers keep their scheme ("Bearer ****").</summary>
        public static readonly IReadOnlyCollection<string> DefaultRedactedNames = new[] {
            "Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie",
            "X-Api-Key", "Api-Key", "ApiKey", "api_key",
            "password", "senha", "secret", "client_secret",
            "access_token", "refresh_token", "id_token",
        };

        public IFileSystem FileSystem { get; set; }
        public string RelativePath { get; set; }

        public bool IncludeRequestHeaders { get; set; }
        public bool IncludeResponseHeaders { get; set; }

        /// <summary>Header, JSON property, form field and query parameter names whose values are masked (case-insensitive).</summary>
        public HashSet<string> RedactedNames { get; } = new HashSet<string>(DefaultRedactedNames, StringComparer.OrdinalIgnoreCase);

        /// <summary>Literal values known to be secret (credentials, keys), masked wherever they appear, including the file name.</summary>
        public HashSet<string> RedactedValues { get; } = new HashSet<string>(StringComparer.Ordinal);

        static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings {
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.Indented,
        };

        /// <summary>
        /// Logs an exchange. <paramref name="response"/> is null when the request failed without an answer
        /// (timeout, connection error); <paramref name="error"/> then says why. Never throws.
        /// </summary>
        public async Task WriteAsync(HttpRequestMessage request, string requestBody, HttpResponseMessage response, string responseBody, TimeSpan elapsed, Exception error) {
            if (FileSystem == null || request == null) {
                return;
            }
            try {
                var now = DateTime.UtcNow;
                var statusCode = response != null ? (int)response.StatusCode : 0;
                var uri = request.RequestUri != null ? RedactUri(request.RequestUri.ToString()) : null;
                var pathForFile = request.RequestUri != null
                    ? RedactUri(request.RequestUri.IsAbsoluteUri ? request.RequestUri.PathAndQuery : request.RequestUri.ToString())
                    : "";
                pathForFile = Regex.Replace(pathForFile.Replace("/", "-"), "\\W", "-");
                if (pathForFile.Length > 150) {
                    pathForFile = pathForFile.Substring(0, 150);
                }

                var entry = new {
                    Method = request.Method.ToString(),
                    Uri = uri,
                    StatusCode = statusCode,
                    ElapsedMilliseconds = elapsed.TotalMilliseconds,
                    RequestHeaders = IncludeRequestHeaders ? RedactHeaders(request.Headers, request.Content?.Headers) : null,
                    ResponseHeaders = IncludeResponseHeaders && response != null ? RedactHeaders(response.Headers, response.Content?.Headers) : null,
                    RequestBody = RedactBody(requestBody, request.Content?.Headers.ContentType?.MediaType),
                    ResponseBody = RedactBody(responseBody, response?.Content?.Headers.ContentType?.MediaType),
                    Error = error != null ? RedactText($"{error.GetType().Name}: {error.Message}") : null,
                };

                await FileSystem.WriteAllTextAsync(
                    $"{RelativePath}/{now:yyyy-MM-dd}/{now:HHmmssfff}{IntEx.GenerateShortRid()}-{statusCode}-{request.Method}-{pathForFile}.json",
                    JsonConvert.SerializeObject(entry, SerializerSettings)
                ).ConfigureAwait(false);
            } catch (Exception x) {
                Fi.Tech.SwallowException(x);
            }
        }

        Dictionary<string, string> RedactHeaders(HttpHeaders headers, HttpHeaders contentHeaders) {
            var retv = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in (headers ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())
                .Concat(contentHeaders ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())) {
                var value = string.Join(", ", header.Value);
                retv[header.Key] = RedactedNames.Contains(header.Key) ? RedactHeaderValue(value) : RedactText(value);
            }
            return retv;
        }

        /// <summary>"Bearer abc" → "Bearer ****": the scheme helps support, the credential does not.</summary>
        static string RedactHeaderValue(string value) {
            var space = value?.IndexOf(' ') ?? -1;
            return space > 0 && !value.Substring(0, space).Contains('=')
                ? $"{value.Substring(0, space)} {RedactedPlaceholder}"
                : RedactedPlaceholder;
        }

        object RedactBody(string body, string mediaType) {
            if (string.IsNullOrEmpty(body)) {
                return body;
            }
            body = RedactText(body);
            if (mediaType != null && mediaType.Equals(FiHttp.ContentTypeFormUrlEncoded, StringComparison.OrdinalIgnoreCase)) {
                return RedactPairs(body);
            }
            JToken json;
            try {
                json = JToken.Parse(body);
            } catch (JsonException) {
                return body;
            }
            if (json is JContainer container) {
                foreach (var property in container.Descendants().OfType<JProperty>().Where(p => RedactedNames.Contains(p.Name)).ToList()) {
                    property.Value = RedactedPlaceholder;
                }
            }
            return json;
        }

        string RedactUri(string uri) {
            var text = RedactText(uri);
            var queryStart = text.IndexOf('?');
            if (queryStart < 0) {
                return text;
            }
            var fragmentStart = text.IndexOf('#', queryStart);
            var query = fragmentStart < 0 ? text.Substring(queryStart + 1) : text.Substring(queryStart + 1, fragmentStart - queryStart - 1);
            return text.Substring(0, queryStart + 1) + RedactPairs(query) + (fragmentStart < 0 ? "" : text.Substring(fragmentStart));
        }

        /// <summary>key=value&amp;key=value (form body or query string), masking values of <see cref="RedactedNames"/>.</summary>
        string RedactPairs(string pairs) {
            return string.Join("&", pairs.Split('&').Select(pair => {
                var eq = pair.IndexOf('=');
                if (eq <= 0) {
                    return pair;
                }
                var name = Uri.UnescapeDataString(pair.Substring(0, eq).Replace('+', ' '));
                return RedactedNames.Contains(name) ? $"{pair.Substring(0, eq)}={RedactedPlaceholder}" : pair;
            }));
        }

        /// <summary>Masks each literal in <see cref="RedactedValues"/>, also in its URL-encoded form.</summary>
        string RedactText(string text) {
            if (string.IsNullOrEmpty(text) || RedactedValues.Count == 0) {
                return text;
            }
            foreach (var value in RedactedValues) {
                if (string.IsNullOrEmpty(value)) {
                    continue;
                }
                text = text.Replace(value, RedactedPlaceholder);
                var escaped = Uri.EscapeDataString(value);
                if (escaped != value) {
                    text = text.Replace(escaped, RedactedPlaceholder);
                }
            }
            return text;
        }
    }
}
