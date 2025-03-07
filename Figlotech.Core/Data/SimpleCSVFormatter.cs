﻿using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Data
{
    public sealed class SimpleCSVFormatter
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
                await Task.Yield();
                return ObjectToRow(headers, c);
            }).Then(1, async (data) => {
                await WriteDataToStreamAsync(data, stream);
            });
        }

        public string[] LineToData(string line) {
            return LineToData(line, false).ToArray();
        }
        private IEnumerable<string> LineToData(string line, bool isInQuote = false) {
            var next = line;

            do {
                if(next.IndexOf(Sep) > -1) {
                    var fragment = next.Substring(0, next.IndexOf(isInQuote ? '\"' : Sep));
                    next = next.Substring(fragment.Length + 1);
                    var idxQuote = fragment.IndexOf('\"');
                    if (idxQuote <= -1) {
                        yield return fragment;
                    } else {
                        fragment = fragment.Substring(0, idxQuote) + fragment.Substring(idxQuote + 1);
                        if(fragment.IndexOf('\"') > -1) {
                            idxQuote = fragment.IndexOf('\"');
                            fragment = fragment.Substring(0, idxQuote) + fragment.Substring(idxQuote + 1);
                            yield return fragment;
                        } else {
                            var idxQuoteInNext = next.IndexOf('\"');
                            if(idxQuoteInNext > -1) {
                                fragment += next.Substring(0, idxQuoteInNext) + next.Substring(idxQuoteInNext + 1, Math.Max(0, next.IndexOf(Sep) - (idxQuoteInNext + 1)));
                            }
                            next = next.Substring(next.IndexOf(Sep) + 1);
                            yield return fragment;
                        }
                    }
                } else {
                    yield return next;
                    yield break;
                    break;
                }
            } while (next.Length > 0);
        }

        public void WriteHeadersToStream(string[] headers, Stream stream) {
            var data = Encoding.UTF8.GetBytes(string.Join($"{Sep}", headers) + "\r\n");
            WriteDataToStream(data, stream);
        }

        public byte[] ObjectToRow(string[] headers, object obj) {
            if(obj == null) {
                return Array.Empty<byte>();
            }
            var t = obj.GetType();
            var data = Encoding.UTF8.GetBytes(string.Join($"{Sep}", headers.Select(x => {
                if(ReflectionTool.TypeContains(t, x)) {
                    var value = ReflectionTool.GetValue(obj, x);
                    if(value == null) {
                        return "";
                    } else if (value is string s) {
                        if(s.Contains(Sep)) {
                            return $"\"{s}\"";
                        } else {
                            return s;
                        }
                    } else if (value.GetType().IsClass) {
                        return "";
                    } else {
                        return value.ToString();
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
        public async Task WriteDataToStreamAsync(byte[] data, Stream stream) {
            await stream.WriteAsync(data, 0, data.Length);
        }
    }
}
