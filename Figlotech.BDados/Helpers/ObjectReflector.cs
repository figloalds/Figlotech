using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers
{
    public class ObjectReflector {
        private object target;
        public ObjectReflector() {
        }
        public ObjectReflector(object o) {
            if (o == null) {
                throw new ArgumentNullException("Object Reflector needs a non-null object to work with.");
            }
            target = o;
            members = ReflectionTool.FieldsAndPropertiesOf(target.GetType()).ToArray();
        }

        public void Slot(object anObject) {
            if(anObject == null) {
                throw new ArgumentNullException("Object Reflector needs a non-null object to work with.");
            }
            target = anObject;
            members = ReflectionTool.FieldsAndPropertiesOf(target.GetType()).ToArray();
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
                var cvType = Nullable.GetUnderlyingType(ReflectionTool.GetTypeOf(key)) ?? ReflectionTool.GetTypeOf(key);
                var o = value;
                if(o != null) {
                    if (ReflectionTool.GetTypeOf(key).IsAssignableFrom(o?.GetType())) {
                        ReflectionTool.SetMemberValue(key, target, o);
                        return;
                    }
                }
                var val = o == null ? null : Convert.ChangeType(o, cvType);
                ReflectionTool.SetMemberValue(key, target, val);
            }
        }

        public object this[String key] {
            get {
                var member = members.FirstOrDefault(m => m.Name == key);
                if (member != null) {
                    return this[member];
                }
                return null;
            }
            set {
                var member = members.FirstOrDefault(m => m.Name == key);
                if(member != null) {
                    this[member] = value;
                }
            }
        }

        MemberInfo[] members = new MemberInfo[0];

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
