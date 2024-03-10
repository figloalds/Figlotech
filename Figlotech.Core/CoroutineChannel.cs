using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public sealed class CoroutineEnumerator<T> : IEnumerator<T>
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
                    return this.Current != null;
                } catch(Exception x) {
                    return false;
                }
            }
        }

        public void Reset() {
            throw new NotSupportedException();
        }
    }

    public sealed class CoroutineChannel<T> : IEnumerable<T>
    {
        private Queue<T> AwaitingOutput = new Queue<T>();
        private Queue<TaskCompletionSource<T>> Next = new Queue<TaskCompletionSource<T>>();
        bool _isClosed = false;

        public bool IsClosed {
            get {
                lock(AwaitingOutput) {
                    return AwaitingOutput.Count == 0 && _isClosed;
                }
            }
        }

        public void Send(T data) {
            if (!IsClosed) {
                lock(Next) {
                    if(Next.Count > 0) {
                        Next.Dequeue().SetResult(data);
                        return;
                    }
                }
                lock (AwaitingOutput) {
                    AwaitingOutput.Enqueue(data);
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
            Next.ForEach(x=> x.SetCanceled());
            _isClosed = true;
        }

        FiAsyncLock ReceiveLock = new FiAsyncLock();
        public async ValueTask<T> Receive() {
            lock (AwaitingOutput) {
                if (AwaitingOutput.Count > 0) {
                    return AwaitingOutput.Dequeue();
                }
            }
            var t = new TaskCompletionSource<T>();
            lock (Next) {
                Next.Enqueue(t);
            }
            return await t.Task.ConfigureAwait(false);
        }

        public IEnumerator GetEnumerator() {
            return new CoroutineEnumerator<T>(this);
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator() {
            return new CoroutineEnumerator<T>(this);
        }
    }
}
