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
    /// <summary>
    /// This class is a Syntax suggar for Figlotech.BDados.DependencySolver
    /// </summary>
    public class DS : DependencyResolver {
        
    }
    /// <summary>
    /// This is my very own approach and study of dependency injection. 
    /// Plan here is to have a dependency solver able to work both in a static singleton level
    /// </summary>
    public class DependencyResolver {

        public static DependencyResolver Default { get; } = new DependencyResolver();

        IDictionary<String, DependencyResolver> solvers = new Dictionary<String, DependencyResolver>();

        public DependencyResolver this[String name] {
            get {
                if (solvers.ContainsKey(name)) {
                    return solvers[name];
                }

                solvers.Add(name, new DependencyResolver(this));
                return solvers[name];
            }
        }

        private DependencyResolver ParentResolver = new DependencyResolver();
        public DependencyResolver(DependencyResolver parent = null) {
            ParentResolver = parent;
            if (parent == null) {
                ParentResolver = DependencyResolver.Default;
            }
        }

        public T New<T>(String contextName = null) where T : IBDadosInjectable {
            try {
                if (contextName != null) {
                    return this[contextName].Resolve<T>();
                }
                var retv = Resolve<T>();
                return retv;
            } catch (Exception) {
                throw new Exception($"You called DS.New for {typeof(T).Name}, but it doesn't have any constructor that takes DependencySolver");
            }
        }

        private Dictionary<Type, Type> BindingMap = new Dictionary<Type, Type>();
        private Dictionary<Type, object> InstanceMap = new Dictionary<Type, Object>();
        private Dictionary<Type, Delegate> FactoryMap = new Dictionary<Type, Delegate>();

        private Type FindBinding(Type t) {
            if (BindingMap.ContainsKey(t)) {
                return BindingMap[t];
            }
            if (ParentResolver != null) {
                return ParentResolver.FindBinding(t);
            }
            return null;
        }
        private object FindInstance(Type t) {
            if (BindingMap.ContainsKey(t)) {
                return InstanceMap[t];
            }
            if (ParentResolver != null) {
                return ParentResolver.FindInstance(t);
            }
            return null;
        }
        private Delegate FindFactory(Type t) {
            if (BindingMap.ContainsKey(t)) {
                return FactoryMap[t];
            }
            if (ParentResolver != null) {
                return ParentResolver.FindFactory(t);
            }
            return null;
        }

        public void AddAbstract<TDependency, TImplementation>() {
            BindingMap.Add(typeof(TDependency), typeof(TImplementation));
        }
        public void AddInstance<TDependency>(TDependency instance) {
            InstanceMap.Add(typeof(TDependency), instance);
        }
        public void AddFactory<TDependency>(Func<TDependency> instance) {
            FactoryMap.Add(typeof(TDependency), instance);
        }

        public T Resolve<T>() {
            return (T)Resolve(typeof(T));
        }

        /// <summary>
        /// <para>
        /// This function scans input object and resolves all fields and properties
        /// that are Interface type with null value and resolves them. The scanner considers
        /// EVERY public interface field or property that is null as a "non-satisfied" dependency
        /// and tries to resolve it.
        /// </para>
        /// <para>
        /// It's shitty, I think the programmer community will hate on me for this, but I don't care
        /// I'm lazy, that's what I am.
        /// </para>
        /// </summary>
        /// <param name="input">The object to be scanned and have its dependencies resolved</param>
        public void SmartResolve(object input) {
            ObjectReflector rflx = new ObjectReflector(input);
            var t = input.GetType();
            var members = ReflectionTool.FieldsAndPropertiesOf(t);

            foreach(var member in members) {
                var type = ReflectionTool.GetTypeOf(member);
                if(type.IsInterface) {
                    if(rflx[member] == null) {
                        rflx[member] = Resolve(type);
                    }
                }
            }

        }

        internal object Resolve(Type t) {
            object value = FindInstance(t);
            if (value != null)
                return value;

            object factory = ((Func<object>) FindFactory(t))?.Invoke();
            if(factory != null) {
                return factory;
            }

            Type res = FindBinding(t);
            var ctorList = res.GetConstructors();
            bool hasParameterLessCtor = false;
            foreach(var ctor in ctorList) {
                var ctorParams = ctor.GetParameters();
                if (ctorParams.Length == 0) {
                    hasParameterLessCtor = true;
                    continue;
                }
                var resolutions = new List<object>();
                foreach(var parameter in ctor.GetParameters()) {
                    try {
                        object o = Resolve(parameter.ParameterType);
                        resolutions.Add(o);
                    } catch(Exception) {
                        continue;
                    }
                }
                return Activator.CreateInstance(t, resolutions.ToArray());
            }

            if(hasParameterLessCtor) {
                return Activator.CreateInstance(t);
            } else {
                throw new BDadosException(String.Format(
                    FTH.Strings.BDIOC_CANNOT_RESOLVE_TYPE, t.Name
                ));
            }
        }
    }
}
