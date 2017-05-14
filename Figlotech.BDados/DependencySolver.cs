using Figlotech.BDados.Entity;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados {
    public class DependencySolver {
        public static DependencySolver Default { get; } = new DependencySolver();

        public DependencySolver(IContextProvider newContext) {
            Context = newContext;
        }
        public DependencySolver() {
            Context = new DefaultContextProvider();
        }

        //public T Instantiate<T>() {
            
        //}

        public IContextProvider Context { get; private set; } = new DefaultContextProvider();
        public void Resolve(object o) {
            Type t = o.GetType();
            foreach (var a in t.GetInterfaces()) {
                if (a.Name.StartsWith("IRequires")) {
                    var role = a.Name.Replace("IRequires", "");
                    var typeName = $"I{role}";
                    Type type = null;
                    foreach(var findType in Assembly.GetAssembly(a).GetTypes()) {
                        if(findType.Name == typeName) {
                            type = findType;
                        }
                    }
                    if(type != null) {
                        var resolution = Context.Get(type);
                        if (resolution != null) {
                            List<MemberInfo> fields = new List<MemberInfo>();
                            fields.AddRange(t.GetFields());
                            fields.AddRange(t.GetProperties());
                            try {
                                foreach (var field in fields) {
                                    if (field.Name == role) {
                                        ReflectionTool.SetValue(o, field.Name, resolution);
                                    }
                                }
                            } catch (Exception) { }
                            continue;
                        }
                    }
                    throw new DependencyException(o.GetType(), type);
                }
            }
        }

        public void PushDefault<T>(T value)  {
            // Makers of .NET
            // Fuck you for not allowing where T : interface
            if (!typeof(T).IsInterface)
                throw new BDadosException("T Should be an interface. Don't let your editor infere this specific generic type.");

            if (value.GetType().GetInterfaces().Contains(typeof(T))) {
                Context.Set(typeof(T).Name, value);
                return;
            }

            // Thinking out loud... This wont ever run. Because its compile time error if value doesn't implement T.
            // Even if we attempt to run this by reflection, the error is thrown before the function can even run.
            // I'll leave this here though, as a totem of learnship.
            throw new BDadosException("Attempting to push an object that does not implement given interface");
        }

        public void AssertDependenciesAreMet(Type[] types, IContextProvider context) {
            var retv = new List<Exception>();
            foreach (var t in types) {
                foreach (var a in t.GetInterfaces()) {
                    if (a.Name.StartsWith("IRequires")) {
                        var role = a.Name.Replace("IRequires", "");
                        var dependedInterface = $"I{role}";
                        var type = Type.GetType(dependedInterface, false);
                        var resolution = Context.Get(type);
                        if (resolution == null) {
                            Debug.WriteLine($"Type {t.Name} depends on {dependedInterface}, not found in this context");
                            retv.Add(new BDadosException($"Type {t.Name} depends on {dependedInterface}, not found in this context"));
                        }
                        List<MemberInfo> fields = new List<MemberInfo>();
                        fields.AddRange(t.GetFields());
                        fields.AddRange(t.GetProperties());
                        try {
                            //foreach (var field in fields) {
                            //    if (field.Name == role) {

                            //    }
                            //}
                        } catch (Exception) { }
                    }
                }
            }
            if (retv.Any())
                throw new AggregateException(retv);
        }
    }
}
