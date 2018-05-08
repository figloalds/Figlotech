
using Figlotech.Core.Extensions;
using IronPython.Hosting;
using Microsoft.Scripting.Hosting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Scripting
{
    public class PythonEnvironment
    {
        ScriptEngine engine;
        ScriptScope scope;
        
        public PythonEnvironment(string[] references, string[] namespaces) {
            engine = Python.CreateEngine();
            scope = engine.CreateScope();

            var lines = new List<String>() {
                "import clr",
                "clr.AddReference('System')",
                "clr.AddReference('Figlotech.Core')",
                "from System import *",
                "from System.Collections.Generic import *",
                "from Figlotech.Core import *"
            };
            references.Iterate(r => lines.Add($"clr.AddReference('{r}')"));

            Execute(lines, null, false);
            scope.RemoveVariable("clr");
        }

        public void PutObject(string str, object obj) {
            scope.SetVariable(str, obj);
        }
        public T GetObject<T>(string str, object obj) {
            return scope.GetVariable<T>(str);
        }

        public void RunFile(string path, Action<Exception> handleException = null) {
            if (!File.Exists(path)) return;
            //Environment.CurrentDirectory = Path.GetDirectoryName(Path.GetFullPath(path));
            //var lines = File.ReadAllLines(path);
            //RunPy(lines);
            var source = engine.CreateScriptSourceFromFile(path);
            try {
                object result = source.Execute(scope);
            } catch (Exception x) {
                handleException?.Invoke(x);
                if(handleException == null) {
                    Console.Error.WriteLine($"Error Executing Script");
                    ExceptionOperations eo = engine.GetService<ExceptionOperations>();
                    string error = eo.FormatException(x);
                    Console.Error.WriteLine(error);
                    var inner = x;
                    while (inner != null) {
                        Console.Error.WriteLine($"[{inner.GetType().Name}] {inner.Message}");
                        Console.Error.WriteLine(inner.StackTrace);
                        inner = inner.InnerException;
                    }
                }
            }
        }

        IEnumerable<string> InteractiveLoop() {
            while (true) {
                Console.Write(">>>");
                var input = new StringBuilder();
                var indent = 0;
                while (true) {
                    var line = Console.ReadLine();
                    input.Append(line);
                    if (line.EndsWith(":")) {
                        indent++;
                    } else {
                        var idLine = line.Count(c => c == '\t');
                        if (idLine < indent)
                            indent--;
                    }
                    if (indent == 0) {
                        break;
                    }
                }
                var cmds = input.ToString();
                if (cmds.Length == 0) {
                    Console.CursorTop--;
                    continue;
                }
                yield return input.ToString();
            }
        }

        public void GoInteractive() {
            Console.Clear();
            Console.WriteLine($"------------------------------------------------");
            Console.WriteLine($"-- Figlotech Scripting Environment");
            Console.WriteLine($"------------------------------------------------");
            var interactive = InteractiveLoop();
            Execute(interactive, null, false);
        }

        public void Execute(IEnumerable<string> cmd, Func<Exception, bool> handleException = null, bool stopOnError = true) {
            var e = cmd.GetEnumerator();
            while(e.MoveNext()) {
                scope.RemoveVariable("clr");
                var source = engine.CreateScriptSourceFromString(e.Current);
                try {
                    object result = source.Execute(scope);
                } catch (Exception x) {
                    if (handleException == null) {
                        Console.Error.WriteLine($"Error Executing Script");
                        ExceptionOperations eo = engine.GetService<ExceptionOperations>();
                        string error = eo.FormatException(x);
                        Console.Error.WriteLine(error);
                        var inner = x;
                        while (inner != null) {
                            Console.Error.WriteLine($"[{inner.GetType().Name}] {inner.Message}");
                            Console.Error.WriteLine(inner.StackTrace);
                            inner = inner.InnerException;
                        }
                    }
                    var doContinue = handleException?.Invoke(x) ?? !stopOnError;
                    if (!doContinue) {
                        return;
                    }
                }
            }
        }
    }
}

