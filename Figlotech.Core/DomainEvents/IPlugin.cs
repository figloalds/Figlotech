using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public interface IPlugin {
        void Execute(int opcode, object obj);
        void OnError(int opcode, object obj, Exception x);
    }

    public static class IExtensibleExtensions {
        public static void ExecutePlugins<TOpCode, T>(this T self, TOpCode opcode) where T : IPluginExtensible<TOpCode> {
            if(self == null || self.EventsHub == null) {
                throw new NullReferenceException($"EventHub was null when trying to execute opcode {opcode} for item of type {typeof(T)}");
            }
            self.EventsHub.ExecutePlugins<T>((int) Convert.ChangeType(opcode, typeof(int)), self);
        }
    }

    public interface IPluginExtensible<TOpCode> {
        DomainEventsHub EventsHub { get; }
    }

    public abstract class Plugin<T> : IPlugin {
        int _opcode;
        public Plugin(int opcode) {
            _opcode = opcode;
        }

        public abstract void Execute(T obj);
        public abstract void OnError(T obj, Exception x);

        public void Execute(int opcode, object obj) {
            if(opcode == _opcode) {
                Execute((T) obj);
            }
        }

        public void OnError(int opcode, object obj, Exception x) {
            if (opcode == _opcode) {
                OnError((T) obj, x);
            }
        }
    }

    public abstract class Plugin<TOpCode, T> : IPlugin where TOpCode: struct, IConvertible {

        TOpCode _opcode;

        public Plugin(TOpCode opcode) {
            _opcode = opcode;
        }

        public abstract bool ShouldExecute { get; }
        public abstract void Execute(T obj);
        public abstract void OnError(T obj, Exception x);

        public void Execute(int opcode, object obj) {
            if (obj == null || obj.GetType() != typeof(T)) {
                return;
            }
            if (!ShouldExecute) {
                return;
            }
            if (opcode == (int) Convert.ChangeType(_opcode, typeof(int))) {
                Execute((T)obj);
            }
        }

        public void OnError(int opcode, object obj, Exception x) {
            if (obj == null || obj.GetType() != typeof(T)) {
                return;
            }
            if (opcode == (int)Convert.ChangeType(_opcode, typeof(int))) {
                OnError((T)obj, x);
            }
        }
    }

}
