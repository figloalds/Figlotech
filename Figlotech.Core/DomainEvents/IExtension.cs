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
        Task Execute(TOpCode opcode, TObject obj);
        Task OnError(TOpCode opcode, TObject obj, Exception x);
    }

    public static class IExtensibleExtensions {
        public static async Task ExecuteExtensionsAsync<TOpCode, T>(this T self, TOpCode opcode) where T : IExtensible<TOpCode> {
            if (self == null || self.EventsHub == null) {
                throw new NullReferenceException($"EventHub was null when trying to execute opcode {opcode} for item of type {typeof(T)}");
            }
            await self.EventsHub.ExecuteExtensionsAsync<TOpCode, T>(opcode, self);
        }
        public static async Task ExecuteExtensionsAsync<TOpCode, T, TOther>(this T self, TOpCode opcode, TOther other) where T : IExtensible<TOpCode> {
            if (self == null || self.EventsHub == null) {
                throw new NullReferenceException($"EventHub was null when trying to execute opcode {opcode} for item of type {typeof(T)}");
            }
            await self.EventsHub.ExecuteExtensionsAsync<TOpCode, TOther>(opcode, other);
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

        public abstract Task Execute(T obj);
        public abstract Task OnError(T obj, Exception x);

        public async Task Execute(int opcode, T obj) {
            if(opcode == _opcode) {
                await Execute((T) obj);
            }
        }

        public async Task OnError(int opcode, T obj, Exception x) {
            if (opcode == _opcode) {
                await OnError((T) obj, x);
            }
        }
    }

    public abstract class Extension<TOpCode, T> : IExtension<TOpCode, T> {

        public Extension() {
        }

        public abstract bool ShouldExecute(TOpCode opcode);
        public abstract Task Execute(T obj);
        public abstract Task OnError(T obj, Exception x);

        public async Task Execute(TOpCode opcode, T obj) {
            if (obj == null || obj.GetType() != typeof(T)) {
                return;
            }
            if (!ShouldExecute(opcode)) {
                return;
            }

            await Execute((T)obj);
        }

        public async Task OnError(TOpCode opcode, T obj, Exception x) {
            if (obj == null || obj.GetType() != typeof(T)) {
                return;
            }
            await OnError((T)obj, x);
        }
    }

}
