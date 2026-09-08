using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public static partial class FiTechCoreExtensions {
        public sealed class ParallelFlowStepInOut<TIn, TOut> : IParallelFlowStepIn<TIn>, IParallelFlowStepOut<TOut>, IAsyncEnumerable<TOut>, IDisposable, IAsyncDisposable {
            WorkQueuer queuer { get; set; }
            Queue<TOut> ValueQueue { get; set; } = new Queue<TOut>();
            readonly object _valueQueueLock = new object();
            Func<TIn, Task<TOut>> SimpleAct { get; set; }
            Func<TIn, FlowYield<TOut>, ValueTask> YieldAct { get; set; }
            Func<Exception, Task> ExceptionHandler { get; set; }
            IParallelFlowStepIn<TOut> ConnectTo { get; set; }
            bool IgnoreOutput { get; set; } = false;
            IParallelFlowStepOut<TIn> Parent { get; set; }

            public TaskCompletionSource<List<TOut>> TaskCompletionSource { get; set; } = new TaskCompletionSource<List<TOut>>();
            public Task<List<TOut>> TaskObj => TaskCompletionSource.Task;

            public void Dispose() {
                queuer.Dispose();
                enumerator.DisposeAsync().ConfigureAwait(false);
            }
            public async ValueTask DisposeAsync() {
                await queuer.DisposeAsync();
                await enumerator.DisposeAsync();
            }

            public ParallelFlowStepInOut(Func<TIn, Task<TOut>> Act, IParallelFlowStepOut<TIn> parent, int maxParallelism) {
                this.SimpleAct = Act;
                this.Parent = parent;
                this.queuer = new WorkQueuer("flow_step_enqueuer", Math.Max(1, maxParallelism));
            }
            public ParallelFlowStepInOut(Func<TIn, FlowYield<TOut>, ValueTask> Act, IParallelFlowStepOut<TIn> parent, int maxParallelism) {
                this.YieldAct = Act;
                this.Parent = parent;
                this.queuer = new WorkQueuer("flow_step_enqueuer", Math.Max(1, maxParallelism));
            }

            public void Put(TIn input) {
                queuer.Enqueue(async () => {
                    if (SimpleAct != null) {
                        var output = await SimpleAct(input).ConfigureAwait(false);
                        if (this.ConnectTo != null) {
                            this.ConnectTo.Put(output);
                            enumerator.Publish(output);
                        } else {
                            if (!IgnoreOutput) {
                                lock (_valueQueueLock)
                                    ValueQueue.Enqueue(output);
                            }
                            enumerator.Publish(output);
                        }
                    } else if (YieldAct != null) {
                        if (this.ConnectTo != null) {
                            await YieldAct(input, new FlowYield<TOut>(this.ConnectTo, enumerator)).ConfigureAwait(false);
                        } else {
                            await YieldAct(input, new FlowYield<TOut>(new QueueFlowStepIn<TOut>(this.ValueQueue), enumerator)).ConfigureAwait(false);
                        }
                    }
                }, async x => {
                    if (ExceptionHandler != null) {
                        await ExceptionHandler(x);
                    }
                });
            }
            Queue<TaskCompletionSource<int>> AlsoQueue { get; set; } = new Queue<TaskCompletionSource<int>>();
            readonly object _alsoQueueLock = new object();
            public ParallelFlowStepInOut<TIn, TOut> Also(Func<FlowYield<TOut>, Task> yieldFn) {
                var src = new TaskCompletionSource<int>();
                lock (_alsoQueueLock)
                    AlsoQueue.Enqueue(src);
                Fi.Tech.FireAndForget(async () => {
                    if (this.ConnectTo != null) {
                        await yieldFn(new FlowYield<TOut>(this.ConnectTo, enumerator)).ConfigureAwait(false);
                    } else {
                        await yieldFn(new FlowYield<TOut>(new QueueFlowStepIn<TOut>(this.ValueQueue), enumerator)).ConfigureAwait(false);
                    }
                    src.SetResult(0);
                });
                return this;
            }

            public ParallelFlowStepInOut<TIn, TOut> Except(Func<Exception, Task> except) {
                this.ExceptionHandler = except;
                return this;
            }

            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Func<TOut, TNext> act)
                => Then(Environment.ProcessorCount, async (i) => {
                    return await Task.Run(() => {
                        return act(i);
                    }).ConfigureAwait(false);
                });
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Func<TOut, Task<TNext>> act)
                => Then(Environment.ProcessorCount, act);
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(int maxParallelism, Func<TOut, Task<TNext>> act) {
                if (maxParallelism < 0) {
                    maxParallelism = Environment.ProcessorCount;
                }
                var retv = new ParallelFlowStepInOut<TOut, TNext>(act, this, maxParallelism);
                ConnectStep(retv);
                return retv;
            }
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Action<TOut, FlowYield<TNext>> act)
                => Then<TNext>(Environment.ProcessorCount, (o, yield) => {
                    act(o, yield);
                    return Fi.Result();
                });
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Func<TOut, FlowYield<TNext>, ValueTask> act)
                => Then(Environment.ProcessorCount, act);
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(int maxParallelism, Func<TOut, FlowYield<TNext>, ValueTask> act) {
                if (maxParallelism < 0) {
                    maxParallelism = Environment.ProcessorCount;
                }
                var retv = new ParallelFlowStepInOut<TOut, TNext>(act, this, maxParallelism);
                ConnectStep(retv);
                return retv;
            }

            public ParallelFlowStepInOut<TOut, TOut> Then(Func<TOut, Task> act)
                => Then(Environment.ProcessorCount, act);
            public ParallelFlowStepInOut<TOut, TOut> Then(int maxParallelism, Func<TOut, Task> act) {
                if (maxParallelism < 0) {
                    maxParallelism = Environment.ProcessorCount;
                }
                var retv = new ParallelFlowStepInOut<TOut, TOut>(async (x) => {
                    await act(x).ConfigureAwait(false);
                    return x;
                }, this, maxParallelism);
                ConnectStep(retv);
                return retv;
            }
            private void ConnectStep(IParallelFlowStepIn<TOut> next) {
                bool sourceCompleted;
                lock (_completionLock) {
                    ConnectTo = next;
                    FlushToConnected();
                    sourceCompleted = _completionSourceSet;
                }
                // A fast source can drain before Then is called. Propagate its completion
                // to late connections after flushing the already-produced values.
                if (sourceCompleted) _ = next.NotifyDoneQueueing();
            }
            public void FlushToConnected() {
                if (this.ConnectTo != null) {
                    lock (_valueQueueLock)
                        while (ValueQueue.Count > 0)
                            this.ConnectTo.Put(ValueQueue.Dequeue());
                }
            }
            bool _completionSourceSet = false;
            readonly object _completionLock = new object();

            public async Task NotifyDoneQueueing() {
                Queue<TaskCompletionSource<int>> alsoQueueSnapshot;
                lock (_alsoQueueLock) {
                    alsoQueueSnapshot = new Queue<TaskCompletionSource<int>>(AlsoQueue);
                    AlsoQueue.Clear();
                }
                while (alsoQueueSnapshot.Count > 0) {
                    await alsoQueueSnapshot.Dequeue().Task.ConfigureAwait(false);
                }
                await queuer.Stop(true).ConfigureAwait(false);
                IParallelFlowStepIn<TOut> connected;
                lock (_completionLock) {
                    if (_completionSourceSet) return;
                    _completionSourceSet = true;
                    connected = ConnectTo;
                }
                if (connected != null) {
                    FlushToConnected();
                    await connected.NotifyDoneQueueing().ConfigureAwait(false);
                }
                List<TOut> results;
                lock (_valueQueueLock) {
                    results = ValueQueue.ToList();
                }
                TaskCompletionSource.TrySetResult(results);
                enumerator.Finish();
            }
            public TaskAwaiter<List<TOut>> GetAwaiter() {
                return TaskCompletionSource.Task.GetAwaiter();
            }
            private readonly ParallelFlowOutEnumerator<TOut> enumerator = new ParallelFlowOutEnumerator<TOut>();
            public IAsyncEnumerator<TOut> GetAsyncEnumerator(CancellationToken cancellation) {
                return enumerator;
            }

        }
    }
}
