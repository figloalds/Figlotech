namespace Figlotech.ECSEngine {
    public static class IComponentExtensions {
        public static T As<T>(this IComponent self) {
            return self is T retv ? retv : default(T);
        }
    }

    public interface IComponent {
        string Name { get; }
    }
}