using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public interface ILogicHook {
        void Execute(int opcode, object obj);
        void OnError(int opcode, object obj, Exception x);
    }

    public abstract class LogicHook<T> : ILogicHook {
        int _opcode;
        public LogicHook(int opcode) {
            _opcode = opcode;
        }

        public abstract void Execute(T evt);
        public abstract void OnError(T evt, Exception x);

        public void Execute(int opcode, object obj) {
            Execute(_opcode, obj);
        }

        public void OnError(int opcode, object obj, Exception x) {
            OnError(_opcode, obj, x);
        }
    }
}
