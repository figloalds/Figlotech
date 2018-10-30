using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.ECSEngine {

    public static class IEntityExtensions {

        public static IEnumerable<T> GetComponents<T>(this IEntity entity) {
            return entity.Components.Where(c => c.GetType() == typeof(T)).Select(c => (T)c);
        }
        public static void AddComponent(this IEntity entity, IComponent component) {
            entity.Components.Add(component);
        }
        public static T GetComponent<T>(this IEntity entity) {
            var retv = entity.Components.FirstOrDefault(c => c.GetType() == typeof(T));
            if(retv == null) {
                return default(T);
            }
            return (T) retv;
        }
    }

    public interface IEntity {
        List<IComponent> Components { get; }
    }

    public class Component : IComponent {
        public String Name { get; private set; }
        public Component(String name) {
            this.Name = name;
        }
    }

    public class FthEntity : IEntity {
        public List<IComponent> Components { get; private set; } = new List<IComponent>();
        public IComponent this[string s] {
            get {
                return this.Components
                    .FirstOrDefault(field => field.Name == s);
            }
            set {
                var field = this.Components
                    .FirstOrDefault(f => f.Name == s);
                if(field != null) {
                    this.Components.Remove(field);
                }
                this.Components.Add(field);
            }
        }

    }
}
