using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Figlotech.Core.Helpers {
    public class ObjectReflector : IEnumerable<KeyValuePair<MemberInfo, object>>, IReadOnlyDictionary<string, object>
    {
        internal object target;

        string[] keys = new string[0];
        object[] values = new object[0];
        public IEnumerable<string> Keys => keys;

        public IEnumerable<object> Values => values;

        public int Count => throw new NotImplementedException();

        public ObjectReflector() {
        }

        private void RefreshKeysAndValues() {
            keys = ReflectionTool.FieldsAndPropertiesOf(target.GetType()).Select(x => x.Name).ToArray();
            values = ReflectionTool.FieldsAndPropertiesOf(target.GetType()).Select(x => ReflectionTool.GetMemberValue(x, target)).ToArray();
        }

        public ObjectReflector(object o) {
            target = o;
            RefreshKeysAndValues();
            if (o == null) {
                throw new ArgumentNullException("Object reflector cannot work with null value.");
            }
        }

        public void Slot(object anObject) {
            target = anObject;
            RefreshKeysAndValues();
        }
        public object Retrieve() {
            return target;
        }

        public bool ContainsKey(String key) {
            return keys.Contains(key); // ReflectionTool.DoesTypeHaveFieldOrProperty(target?.GetType(), key);
        }

        public object this[KeyValuePair<MemberInfo, object> key] {
            get {
                return ReflectionTool.GetMemberValue(key.Key, target);
            }
            set {
                ReflectionTool.SetMemberValue(key.Key, target, value);
            }
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
                if (target is IDictionary<String, object>) {
                    var t = (IDictionary<String, object>)target;
                    if (t.ContainsKey(key)) {
                        return ((Dictionary<String, object>)target)[key];
                    }
                }
                return ReflectionTool.GetValue(target, key);
            }
            set {
                if (target is IDictionary<String, object>) {
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
        public IEnumerator<KeyValuePair<MemberInfo, object>> GetEnumerator() {
            foreach(var a in ReflectionTool.FieldsAndPropertiesOf(target.GetType())) {
                yield return new KeyValuePair<MemberInfo, object>(a, ReflectionTool.GetMemberValue(a, target));
            }
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        public bool TryGetValue(string key, out object value) {
            if(ContainsKey(key)) {
                value = ReflectionTool.GetValue(target, key);
                return true;
            }
            value = null;
            return false;
        }

        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator() {
            foreach (var a in ReflectionTool.FieldsAndPropertiesOf(target.GetType())) {
                yield return new KeyValuePair<string, object>(a.Name, ReflectionTool.GetMemberValue(a, target));
            }
        }
    }
}
