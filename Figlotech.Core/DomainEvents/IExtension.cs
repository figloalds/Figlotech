using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{

    public interface IExtension {
    }
    public interface IExtension<TOpCode, TObject> : IExtension {
        void Execute(TOpCode opcode, TObject obj);
        void OnError(TOpCode opcode, TObject obj, Exception x);
    }

    public static class IExtensibleExtensions {
        public static void ExecuteExtensions<TOpCode, T>(this T self, TOpCode opcode) where T : IExtensible<TOpCode> {
            if (self == null || self.EventsHub == null) {
                throw new NullReferenceException($"EventHub was null when trying to execute opcode {opcode} for item of type {typeof(T)}");
            }
            self.EventsHub.ExecuteExtensions<TOpCode, T>(opcode, self);
        }
        public static void ExecuteExtensions<TOpCode, T, TOther>(this T self, TOpCode opcode, TOther other) where T : IExtensible<TOpCode> {
            if (self == null || self.EventsHub == null) {
                throw new NullReferenceException($"EventHub was null when trying to execute opcode {opcode} for item of type {typeof(T)}");
            }
            self.EventsHub.ExecuteExtensions<TOpCode, TOther>(opcode, other);
        }
    }

    public interface IExtensible<TOpCode> {
        DomainEventsHub EventsHub { get; }
    }

    public abstract class Extension<T> : IExtension<int, T> {
        int _opcode;
        public Extension(int opcode) {
            _opcode = opcode;
        }

        public abstract void Execute(T obj);
        public abstract void OnError(T obj, Exception x);

        public void Execute(int opcode, T obj) {
            if(opcode == _opcode) {
                Execute((T) obj);
            }
        }

        public void OnError(int opcode, T obj, Exception x) {
            if (opcode == _opcode) {
                OnError((T) obj, x);
            }
        }
    }

    public abstract class Extension<TOpCode, T> : IExtension<TOpCode, T> {

        public Extension() {
        }

        public abstract bool ShouldExecute(TOpCode opcode);
        public abstract void Execute(T obj);
        public abstract void OnError(T obj, Exception x);

        public void Execute(TOpCode opcode, T obj) {
            if (obj == null || obj.GetType() != typeof(T)) {
                return;
            }
            if (!ShouldExecute(opcode)) {
                return;
            }

            Execute((T)obj);
        }

        public void OnError(TOpCode opcode, T obj, Exception x) {
            if (obj == null || obj.GetType() != typeof(T)) {
                return;
            }
            OnError((T)obj, x);
        }
    }

}
