using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public class CoroutineEnumerator<T> : IEnumerator<T>
    {
        CoroutineChannel<T> _channel;
        public CoroutineEnumerator(CoroutineChannel<T> channel) {
            this._channel = channel;
        }

        public T Current { get; private set; }

        object IEnumerator.Current => this.Current;

        public void Dispose() {

        }

        public bool MoveNext() {
            if (_channel.IsClosed) {
                return false;
            } else {
                try {
                    this.Current = _channel.Receive().ConfigureAwait(false).GetAwaiter().GetResult();
                    return true;
                } catch(Exception x) {
                    return false;
                }
            }
        }

        public void Reset() {
            throw new NotSupportedException();
        }
    }

    public class CoroutineChannel<T> : IEnumerable<T>
    {
        private Queue<T> Buffer = new Queue<T>();
        private TaskCompletionSource<T> Next = new TaskCompletionSource<T>();
        bool _outputIsBlocking = false;
        bool _isClosed = false;

        public bool IsClosed => Buffer.Count == 0 && _isClosed;

        public void Send(T data) {
            if (!IsClosed) {
                if(_outputIsBlocking) {
                    Next.SetResult(data);
                    Next = new TaskCompletionSource<T>();
                } else {
                    Buffer.Enqueue(data);
                }
            } else {
                throw new Exception("This channel is closed");
            }
        }

        public void SendLast(T data) {
            _isClosed = true;
            Send(data);
        }

        public void Close() {
            Next.SetCanceled();
            _isClosed = true;
        }

        public async ValueTask<T> Receive() {
            if (Buffer.Any()) {
                return Buffer.Dequeue();
            }
            _outputIsBlocking = true;
            var retv = await Next.Task;
            _outputIsBlocking = false;
            return retv;
        }

        public IEnumerator GetEnumerator() {
            return new CoroutineEnumerator<T>(this);
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator() {
            return new CoroutineEnumerator<T>(this);
        }
    }
}
