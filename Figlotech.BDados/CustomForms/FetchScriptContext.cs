using Figlotech.BDados.CustomForms.FetchScript;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.CustomForms {

    public class FetchScriptContext {

        Object flow = null;
        FetchScriptContext parent = null;

        public List<FetchScriptFunction> functions = new List<FetchScriptFunction>();

        public Dictionary<String, Object> Scope = new Dictionary<String, Object>();

        public Object ReadScope(String Key) {
            if (Scope.ContainsKey(Key))
                return Scope[Key];
            if (parent != null)
                return parent.ReadScope(Key);
            return null;
        }

        public void WriteScope(String Key, Object Value) {
            Scope[Key] = Value;
        }

        public FetchScriptContext(FetchScriptContext ts = null, Object initObj = null) {
            if (ts != null)
                parent = ts;
            flow = initObj;
        }

        int blockMode = 0;
        List<String> block = new List<String>();
        String blockType;



        public void RunCommands(String Commands, Object initObj = null) {

            if (Commands.Trim().ToLower() == "foreach") {
                if(! (flow is IEnumerable)) {
                    Console.WriteLine($"Expected {flow?.GetType().Name} to be an enumerable type.");
                }
                blockMode++;
                if (blockMode == 1) {
                    blockType = "foreach";
                    return;
                }
            }
            if (blockMode > 0) {
                if (Commands.Trim().ToLower() == "end") {
                    blockMode--;
                }
                if (blockMode == 0) {
                    switch (blockType) {
                        case "foreach":
                            if (flow is IEnumerable) {
                                foreach (var ct in (IEnumerable)flow) {
                                    var ctx = new FetchScriptContext(this, ct);
                                    // scope is copied down, changes are scoped unless explicitly called.
                                    foreach (var a in block) {
                                        ctx.RunCommands(a);
                                    }
                                }
                            }
                            break;
                    }
                    block.Clear();
                    return;
                }
                block.Add(Commands);
                return;
            }
            String[] cmdsBreak = Commands.Split('|');
            foreach (var a in cmdsBreak) {
                var fsa = new FetchScriptAPI();
                String[] cmds = CmdStringToArray(a.Trim());
                var method = typeof(FetchScriptAPI).GetMethods().Where((m) => m.Name.ToLower() == (cmds[0]).ToLower()).FirstOrDefault();
                object methodTarget = fsa;
                if (null == method) {
                    method = flow.GetType().GetMethods().Where((m) => m.Name.ToLower() == (cmds[0].ToLower())).FirstOrDefault();
                    if (null == method) {
                        Console.WriteLine($"Command not found {Commands}");
                        return;
                    }
                    methodTarget = flow;
                }
                var args = new List<Object>();
                var cmdargs = cmds.Skip(1).ToArray();
                foreach (var arg in cmdargs) {
                    var match = Regex.Match(arg.Trim(), @"^\[(\w+)\]$", RegexOptions.Multiline);
                    if (match.Success) {
                        var variname = match.Groups[1].Value;
                        args.Add(ReadScope(variname));
                    } else {
                        args.Add(arg);
                    }
                }
                if (method.GetParameters().Length > args.Count) {
                    args.Add(flow);
                }
                if (method.ReturnType != null) {
                    flow = method.Invoke(fsa, args.ToArray());
                    Console.WriteLine($"-> {flow?.ToString()}");
                } else {
                    method.Invoke(fsa, args.ToArray());
                }
            }
        }


        internal String[] CmdStringToArray(String inputStr) {
            String[] input = inputStr.Split(' ');
            List<String> li = new List<String>();
            Boolean AppendingString = false;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < input.Length; i++) {
                if (AppendingString) {
                    sb.Append(" " + input[i].Replace("\"", ""));
                }
                if (input[i].Contains('\"')) {
                    input[i] = input[i].Replace("\"", "");
                    AppendingString = !AppendingString;
                    if (!AppendingString) {
                        li.Add(sb.ToString());
                        sb = new StringBuilder();
                        continue;
                    } else {
                        sb.Append(input[i]);
                    }
                }
                if (AppendingString) {
                    continue;
                }
                li.Add(input[i]);
            }
            return li.ToArray();
        }

        public static List<String> BuildBlock() {
            List<String> CommandsBuffer = new List<String>();
            while (true) {
                try {
                    FTH.GlobalQueuer.Enqueue(() => { 
                        for (int i = 0; i < 2; i++) {
                            Console.Beep(1000, 200);
                            Thread.Sleep(TimeSpan.FromMilliseconds(20));
                        }
                    });
                    Console.Write("FI> ");
                    String Commands = Console.ReadLine();
                    if (Commands == "end") {
                        return CommandsBuffer;
                    }
                    CommandsBuffer.Add(Commands);
                } catch (Exception x) {
                    var ex = x.InnerException;
                    int maxRec = 10;
                    while (ex != null && maxRec-- > 0) {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                        ex = ex.InnerException;
                    }
                }
            }
        }

        public void RunInteractive() {
            while (true) {
                try {
                    FTH.GlobalQueuer.Enqueue(() => {
                        for (int i = 0; i < 2; i++) {
                            Console.Beep(1000, 200);
                            Thread.Sleep(TimeSpan.FromMilliseconds(20));
                        }
                    });
                    Console.Write("FI> ");
                    String Commands = Console.ReadLine();
                    if (Commands.StartsWith("define")) {
                        List<String> cmdSplit = Commands.Split(' ').ToList();
                        functions.Add(new FetchScriptFunction {
                            Name = cmdSplit[1],
                            Parameters = cmdSplit.Skip(2).ToList(),
                            Commands = FetchScriptContext.BuildBlock()
                        });
                        return;
                    }
                    //if(Commands == "end") {
                    //    return;
                    //}
                    RunCommands(Commands);
                } catch (Exception x) {
                    var ex = x.InnerException;
                    int maxRec = 10;
                    while (ex != null && maxRec-- > 0) {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                        ex = ex.InnerException;
                    }
                }
            }
        }
    }
}
