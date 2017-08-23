using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace Figlotech.BDados.Helpers {
    public class ObjectReflector {
        private object target;
        public ObjectReflector() {
        }
        public ObjectReflector(object o) {
            target = o;
            if (o == null) {
                throw new ArgumentNullException("Object reflector cannot work with null value.");
            }
        }

        public void Slot(object anObject) {
            target = anObject;
        }
        public object Retrieve() {
            return target;
        }

        public bool ContainsKey(String key) {
            return ReflectionTool.FieldsAndPropertiesOf(target.GetType())
                .Any(m => m.Name == key);
        }


        public object this[MemberInfo key] {
            get {
                return ReflectionTool.GetMemberValue(key, target);
            }
            set {
                ReflectionTool.SetMemberValue(key, target, value);
            }
        }

        public object this[String key] {
            get {
                if (target is IDictionary<String, object> || target is ExpandoObject) {
                    var t = (IDictionary<String, object>)target;
                    if (t.ContainsKey(key)) {
                        return ((Dictionary<String, object>)target)[key];
                    }
                }
                return ReflectionTool.GetValue(target, key);
            }
            set {
                if (target is IDictionary<String, object> || target is ExpandoObject) {
                    var t = (IDictionary<String, object>)target;
                    ((IDictionary<String, object>)target)[key] = value;
                    return;
                }
                ReflectionTool.SetValue(target, key, value);
            }
        }

        public static object Build(Type input, Action<ObjectReflector> workAction) {
            ObjectReflector manipulator = new ObjectReflector();
            var target = Activator.CreateInstance(input);
            manipulator.Slot(target);
            workAction(manipulator);
            return manipulator.Retrieve();
        }

        public static object Open(object input, Action<ObjectReflector> workAction) {
            ObjectReflector manipulator = new ObjectReflector();
            manipulator.Slot(input);
            workAction(manipulator);
            return manipulator.Retrieve();
        }
    }
}
