using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Figlotech.Core.Tests {
    public sealed class FiHttpRequestLoggingTests : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), "fihttp-log-" + Guid.NewGuid().ToString("N"));

        public FiHttpRequestLoggingTests() {
            Directory.CreateDirectory(_dir);
        }

        public void Dispose() {
            try { Directory.Delete(_dir, true); } catch { }
        }

        FiHttpRequestLogging NovoLogging() => new FiHttpRequestLogging {
            FileSystem = new FileAccessor(_dir),
            RelativePath = "RespAPIExterna/Teste",
            IncludeRequestHeaders = true,
            IncludeResponseHeaders = true,
        };

        (string Caminho, string Texto, JObject Json) UnicoLog() {
            var arquivos = Directory.GetFiles(_dir, "*.json", SearchOption.AllDirectories);
            Assert.Single(arquivos);
            var texto = File.ReadAllText(arquivos[0]);
            return (Path.GetRelativePath(_dir, arquivos[0]).Replace('\\', '/'), texto, JObject.Parse(texto));
        }

        [Fact]
        public async Task WriteAsync_MascaraAuthorizationPreservandoEsquema() {
            var log = NovoLogging();
            using var req = new HttpRequestMessage(HttpMethod.Get, "https://api.exemplo.com/v1/itens");
            req.Headers.Authorization = new AuthenticationHeaderValue("Basic", "dXN1YXJpbzpzZW5oYQ==");
            req.Headers.Add("client_id", "meu-client-id");

            await log.WriteAsync(req, null, null, null, TimeSpan.FromMilliseconds(5), null);

            var (_, texto, json) = UnicoLog();
            Assert.DoesNotContain("dXN1YXJpbzpzZW5oYQ==", texto);
            Assert.Equal("Basic ****", (string)json["RequestHeaders"]!["Authorization"]!);
            Assert.Equal("meu-client-id", (string)json["RequestHeaders"]!["client_id"]!);
        }

        [Fact]
        public async Task WriteAsync_MascaraHeadersPorNomeComoSenha() {
            var log = NovoLogging();
            using var req = new HttpRequestMessage(HttpMethod.Post, "https://api.exemplo.com/pesquisa");
            req.Headers.Add("login", "usuario");
            req.Headers.Add("senha", "segredo123");

            await log.WriteAsync(req, null, null, null, TimeSpan.Zero, null);

            var (_, texto, json) = UnicoLog();
            Assert.DoesNotContain("segredo123", texto);
            Assert.Equal("****", (string)json["RequestHeaders"]!["senha"]!);
            Assert.Equal("usuario", (string)json["RequestHeaders"]!["login"]!);
        }

        [Fact]
        public async Task WriteAsync_MascaraCamposJsonEmQualquerProfundidade() {
            var log = NovoLogging();
            using var req = new HttpRequestMessage(HttpMethod.Post, "https://auth.exemplo.com/token");
            using var resp = new HttpResponseMessage(HttpStatusCode.OK) { RequestMessage = req };

            await log.WriteAsync(req,
                "{\"client_id\":\"abc\",\"credenciais\":{\"client_secret\":\"s3cr3t\"}}",
                resp,
                "{\"access_token\":\"tok-123\",\"expires_in\":3600,\"itens\":[{\"password\":\"p\"}]}",
                TimeSpan.Zero, null);

            var (_, texto, json) = UnicoLog();
            Assert.DoesNotContain("s3cr3t", texto);
            Assert.DoesNotContain("tok-123", texto);
            Assert.Equal("abc", (string)json["RequestBody"]!["client_id"]!);
            Assert.Equal("****", (string)json["RequestBody"]!["credenciais"]!["client_secret"]!);
            Assert.Equal("****", (string)json["ResponseBody"]!["access_token"]!);
            Assert.Equal(3600, (int)json["ResponseBody"]!["expires_in"]!);
            Assert.Equal("****", (string)json["ResponseBody"]!["itens"]![0]!["password"]!);
        }

        [Fact]
        public async Task WriteAsync_MascaraCamposDeFormularioEQueryString() {
            var log = NovoLogging();
            using var req = new HttpRequestMessage(HttpMethod.Post, "https://auth.exemplo.com/token?access_token=tok-url&pagina=2");
            req.Content = new StringContent("grant_type=client_credentials&client_id=abc&client_secret=s3cr3t", Encoding.UTF8, "application/x-www-form-urlencoded");

            await log.WriteAsync(req, await req.Content.ReadAsStringAsync(), null, null, TimeSpan.Zero, null);

            var (caminho, texto, json) = UnicoLog();
            Assert.DoesNotContain("s3cr3t", texto);
            Assert.DoesNotContain("tok-url", texto);
            Assert.DoesNotContain("tok-url", caminho);
            Assert.Equal("grant_type=client_credentials&client_id=abc&client_secret=****", (string)json["RequestBody"]!);
            Assert.Equal("https://auth.exemplo.com/token?access_token=****&pagina=2", (string)json["Uri"]!);
        }

        [Fact]
        public async Task WriteAsync_MascaraValoresLiteraisEmQualquerParte() {
            var log = NovoLogging();
            log.RedactedValues.Add("chave-da-loja");
            using var req = new HttpRequestMessage(HttpMethod.Get, "https://api.exemplo.com/lojas/chave-da-loja/itens");
            req.Headers.Add("X-Loja", "chave-da-loja");
            using var resp = new HttpResponseMessage(HttpStatusCode.BadRequest) { RequestMessage = req };

            await log.WriteAsync(req, null, resp, "erro para chave-da-loja", TimeSpan.Zero, null);

            var (caminho, texto, _) = UnicoLog();
            Assert.DoesNotContain("chave-da-loja", texto);
            Assert.DoesNotContain("chave-da-loja", caminho);
        }

        [Fact]
        public async Task WriteAsync_RegistraFalhaSemResposta() {
            var log = NovoLogging();
            using var req = new HttpRequestMessage(HttpMethod.Post, "https://api.exemplo.com/lotes");

            await log.WriteAsync(req, "[1,2]", null, null, TimeSpan.FromSeconds(60), new TaskCanceledException("timeout"));

            var (caminho, _, json) = UnicoLog();
            Assert.StartsWith("RespAPIExterna/Teste/", caminho);
            Assert.Contains("-0-POST-", caminho);
            Assert.Equal(0, (int)json["StatusCode"]!);
            Assert.Contains("timeout", (string)json["Error"]!);
            Assert.Equal(2, ((JArray)json["RequestBody"]!).Count);
        }

        [Fact]
        public async Task WriteAsync_OmiteHeadersQuandoDesligado() {
            var log = NovoLogging();
            log.IncludeRequestHeaders = false;
            using var req = new HttpRequestMessage(HttpMethod.Get, "https://api.exemplo.com/");
            req.Headers.Add("X-Qualquer", "1");

            await log.WriteAsync(req, null, null, null, TimeSpan.Zero, null);

            var (_, _, json) = UnicoLog();
            Assert.Null(json["RequestHeaders"]);
        }

        [Fact]
        public async Task FiHttp_RegistraFalhaDeConexaoComCabecalhosMascarados() {
            var api = new FiHttp("http://127.0.0.1:1") { Logging = NovoLogging() };
            api["Authorization"] = "Bearer tok-123";

            using var result = await api.Post("lotes", new { client_secret = "s3cr3t", itens = 2 });

            Assert.Equal(0, (int)result.StatusCode);
            var (_, texto, json) = UnicoLog();
            Assert.DoesNotContain("tok-123", texto);
            Assert.DoesNotContain("s3cr3t", texto);
            Assert.Equal("Bearer ****", (string)json["RequestHeaders"]!["Authorization"]!);
            Assert.Equal(2, (int)json["RequestBody"]!["itens"]!);
            Assert.NotNull(json["Error"]);
        }

        [Fact]
        public void RedactedNames_PodemSerEstendidosEReduzidos() {
            var log = NovoLogging();
            log.RedactedNames.Add("X-Loja");
            log.RedactedNames.Remove("senha");
            Assert.Contains("x-loja", log.RedactedNames);
            Assert.DoesNotContain("SENHA", log.RedactedNames);
            Assert.Contains("Authorization", log.RedactedNames);
        }
    }
}
