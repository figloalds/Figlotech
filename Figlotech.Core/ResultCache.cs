using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core;
public sealed class ResultCache {

    IFileSystem fileSystem { get; set; }

    public ResultCache(IFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    private string SerializeObjectValue(object o) {
        if(o == null) {
            return "<null>";
        }
        if(o is DateTime dt) {
            return dt.Ticks.ToString();
        }
        if(o is IDataObject ido) {
            return $"[{o.GetType().Name}::{ido.RID}]";
        }
        if(o is IQueryBuilder qb) {
            return $"{{{qb.GetCommandText()}}}({JsonConvert.SerializeObject(qb.GetParameters(), Formatting.None)})";
        }
        var t = o.GetType();
        if (t.FullName.StartsWith("System.Collections.Generic.List`1")) {
            StringBuilder sb = new StringBuilder();
            var any = false;
            var enny = o.GetType().GetMethod("GetEnumerator").Invoke(o, new object[0]);
            while((bool) enny.GetType().GetMethod("MoveNext").Invoke(enny, new object[0])) {
                if(any) {
                    sb.Append(",");
                } else {
                    any = true;
                }
                sb.Append(SerializeObjectValue(enny.GetType().GetProperty("Current").GetValue(enny)));
            }
            var retv = sb.ToString();
            return retv;
        }
        return o.ToString();
    }

    private string SerializeArgumentValue(Expression x) {
        if(x is ConstantExpression cex) {
            return SerializeObjectValue(cex.Value);
        }
        if(x is MemberExpression mex) {
            if(mex.Expression is ConstantExpression cex2) {
                return SerializeObjectValue(ReflectionTool.GetMemberValue(mex.Member, cex2.Value));
            }
            return SerializeArgumentValue(mex.Expression);
        }
        if(x is UnaryExpression unex) {
            if(unex.Operand.NodeType == ExpressionType.Lambda) {
                return unex.Operand.ToString();
            }
            if(unex.NodeType == ExpressionType.Convert) {
                return SerializeArgumentValue(unex.Operand);
            }
            return unex.ToString();
        }
        if(x.CanReduce) {
            return SerializeArgumentValue(x.Reduce());
        }
        throw new Exception("Expressão não suportada pelo mecanismo de cache");
    }

    private string SerializeMethodCallLabel(MethodCallExpression expr) {
        return $"{(expr.Object is MethodCallExpression sub ? SerializeMethodCallLabel(sub) + "." : "")}{expr.Method.DeclaringType.Name}::{expr.Method.Name}({string.Join(',', expr.Arguments.Select(x=> SerializeArgumentValue(x)))})";
    }

    private string HashLabel(string label) {
        return Convert.ToBase64String(Fi.Tech.ComputeHash(Encoding.UTF8.GetBytes(label))).Replace("/","_").Replace("=","-");
    }

    FiAsyncMultiLock multilock = new FiAsyncMultiLock();

    public async Task<T> RunAsync<T>(Expression<Func<Task<T>>> expression, bool bypass = false) {
        if(bypass) {
            return await expression.Compile().Invoke();
        }
        var label = SerializeMethodCallLabel(expression.Body as MethodCallExpression);
        var hash = HashLabel(label);
        using var l = await multilock.Lock(hash);

        var dict = new Dictionary<string, object>();
        if (await fileSystem.ExistsAsync(hash).ConfigureAwait(false)) {
            try {
                dict = JsonConvert.DeserializeObject<Dictionary<string, object>>(
                    Encoding.UTF8.GetString(await Fi.Tech.GzipInflateAsync(await fileSystem.ReadAllBytesAsync(hash)))
                );
            } catch (Exception x) {

            }
        }

        if (!dict.ContainsKey(label)) {
            var task = expression.Compile().Invoke();
            dict[label] = await task;

            await fileSystem.WriteAllBytesAsync(hash,
                await Fi.Tech.GzipDeflateAsync(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(dict)))
            );

            return (T)dict[label];
        }

        if (dict.ContainsKey(label)) {
            if (dict[label] is JObject) {
                var retv = (dict[label] as JObject).ToObject(typeof(T));
                return retv is T ? (T)retv : default(T);
            }
            if (dict[label] is JArray) {
                var retv = (dict[label] as JArray).ToObject(typeof(T));
                return retv is T ? (T)retv : default(T);
            }
        }

        return default(T);
    }

    public async Task<T> Run<T>(Expression<Func<T>> expression, bool bypass = false) {
        if (bypass) {
            return expression.Compile().Invoke();
        }
        var label = SerializeMethodCallLabel(expression.Body as MethodCallExpression);
        var hash = HashLabel(label);
        using var l = multilock.Lock(hash).GetAwaiter().GetResult();

        var dict = new Dictionary<string, object>();
        if(await fileSystem.ExistsAsync(hash).ConfigureAwait(false)) {
            try {
                dict = JsonConvert.DeserializeObject<Dictionary<string, object>>(
                    Encoding.UTF8.GetString(Fi.Tech.GzipInflate(fileSystem.ReadAllBytes(hash)))
                );
            } catch(Exception x) {

            }
        }

        if(!dict.ContainsKey(label)) {
            dict[label] = expression.Compile().Invoke();

            fileSystem.WriteAllBytes(hash, 
                Fi.Tech.GzipDeflate(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(dict)))
            );

            return (T) dict[label];
        }

        if(dict.ContainsKey(label)) {
            if (dict[label] is JObject) {
                var retv = (dict[label] as JObject).ToObject(typeof(T));
                return retv is T ? (T) retv : default(T);
            }
            if (dict[label] is JArray) {
                var retv = (dict[label] as JArray).ToObject(typeof(T));
                return retv is T ? (T)retv : default(T);
            }
        }

        return default(T);
    }
}
