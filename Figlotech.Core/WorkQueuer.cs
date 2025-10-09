using Figlotech.Core.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public enum WorkJobRequestStatus {
        Queued,
        Running,
        Failed,
        Finished
    }

    public sealed class JobProgress {
        public String Status;
        public int TotalSteps;
        public int CompletedSteps;
    }

    public sealed class WorkJobException : Exception {
        public string EnqueuingContextStackTrace { get; private set; }
        public WorkJobExecutionStat WorkJobDetails { get; private set; }
        public WorkJobException(string message, WorkJobExecutionRequest job, Exception inner) : base(message, inner) {
            this.EnqueuingContextStackTrace = job.StackTrace?.ToString();
        }
    }

    public sealed class WorkJobExecutionRequest : IDisposable {
        public int id = ++idGen;
        private static int idGen = 0;

        public WorkJob WorkJob { get; internal set; }
        public CancellationTokenSource Cancellation { get; internal set; }
        public CancellationToken RequestCancellation { get; internal set; }
        public Activity LoggingActivity { get; set; }

        private bool _disposed;
        public WorkJobExecutionRequest(WorkJob job, CancellationToken? requestCancellation = null) {
            WorkJob = job;
            Cancellation = new CancellationTokenSource();
            RequestCancellation = requestCancellation ?? CancellationToken.None;
            Status = WorkJobRequestStatus.Queued;
        }

        public WorkJobRequestStatus Status { get; internal set; }
        public DateTime? EnqueuedTime { get; internal set; }
        public DateTime? DequeuedTime { get; internal set; }
        public DateTime? CompletedTime { get; internal set; }
        public TimeSpan? TimeToComplete { get; internal set; }
        public TimeSpan? TimeInQueue { get; internal set; }
        internal WorkQueuer WorkQueuer { get; set; }

        internal readonly TaskCompletionSource<int> _tcsNotifyDequeued = new(TaskCreationOptions.RunContinuationsAsynchronously);

        readonly TaskCompletionSource<int> _taskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource<int> TaskCompletionSource => _taskCompletionSource;

        public ConfiguredTaskAwaitable<int>.ConfiguredTaskAwaiter GetAwaiter() {
            return GetAwaiterInternal().ConfigureAwait(false).GetAwaiter();
        }

        public async Task WaitForDequeue() {
            await _tcsNotifyDequeued.Task.ConfigureAwait(false);
        }

        public async Task<int> GetAwaiterInternal() {
            if (this.EnqueuedTime == null) {
                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
                throw new InternalProgramException($"Trying to wait for a job that was not enqueued: \"{WorkJob.Name}\"");
            }

            return await TaskCompletionSource.Task.ConfigureAwait(false);
        }

        public async Task ContinueWith(Action<Task<int>> action) {
            await TaskCompletionSource.Task.ContinueWith(action).ConfigureAwait(false);
        }

        public StackTrace StackTrace { get; internal set; }

        public void Dispose() {
            if (!_disposed) {
                Cancellation.Dispose();
                LoggingActivity?.Dispose();
                _disposed = true;
            }
        }
        ~WorkJobExecutionRequest() {
            Dispose();
        }
    }

    public sealed class WorkJob {
        public Func<CancellationToken, ValueTask> action;
        public Func<bool, ValueTask> finished;
        public Func<Exception, ValueTask> handling;

        static readonly Func<Func<ValueTask>, Func<CancellationToken, ValueTask>> ConvertActionFromAbsentOptionalParameter
            = fn => async (ignore) => await fn().ConfigureAwait(false);

        public String Name { get; set; } = null;
        public String Description { get; set; } = null;
        public bool AllowTelemetry { get; set; } = true;

        public Dictionary<string, object> AdditionalTelemetryTags { get; private set; } = new Dictionary<string, object>();

        public ValueTask ActionTask { get; internal set; }

        public WorkJob(Func<CancellationToken, ValueTask> method, Func<Exception, ValueTask> errorHandling = null, Func<bool, ValueTask> actionWhenFinished = null) {
            action = method;
            finished = actionWhenFinished;
            handling = errorHandling;
        }
        public WorkJob(Func<ValueTask> method, Func<Exception, ValueTask> errorHandling = null, Func<bool, ValueTask> actionWhenFinished = null) {
            action = ConvertActionFromAbsentOptionalParameter(method);
            finished = actionWhenFinished;
            handling = errorHandling;
        }

        ~WorkJob() {

        }
    }

    public sealed class WorkJobExecutionStat {
        public string Description { get; set; }
        public DateTime? EnqueuedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public decimal TimeWaiting { get; set; }
        public decimal TimeInExecution { get; set; }
        public Dictionary<string, object> AdditionalTelemetryTags { get; set; }
        public StackTrace SchedulingContextStackTrace { get; set; }
        public WorkJobExecutionStat(WorkJobExecutionRequest x) {
            Description = x.WorkJob.Name;
            EnqueuedAt = x.EnqueuedTime;
            StartedAt = x.DequeuedTime;
            TimeWaiting = (decimal)((x.DequeuedTime ?? DateTime.UtcNow) - (x.EnqueuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
            TimeInExecution = (decimal)(DateTime.UtcNow - (x.DequeuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
            SchedulingContextStackTrace = x?.StackTrace;
            AdditionalTelemetryTags = x.WorkJob.AdditionalTelemetryTags;
        }
    }


    public sealed class WorkQueuer : IDisposable, IAsyncDisposable {
        public static int qid_increment = 0;
        private int __qid = ++qid_increment;
        public int QID => __qid;
        public string Name { get; set; }

        public event Func<WorkJobExecutionRequest, Task> OnWorkEnqueued;
        public event Func<WorkJobExecutionRequest, Task> OnWorkDequeued;
        public event Func<WorkJobExecutionRequest, Task> OnWorkComplete;
        public event Func<WorkJobExecutionRequest, Exception, Exception, Task> OnExceptionInHandler;

        public Dictionary<string, object> DefaultLoggingTags { get; } = new Dictionary<string, object>();

        // Overflow queue kept in strict FIFO order
        private readonly Queue<WorkJobExecutionRequest> _overflowQueue = new Queue<WorkJobExecutionRequest>();
        private readonly object _overflowLock = new object();

        // Held when not Active, flushed on Start()
        private readonly List<WorkJobExecutionRequest> HeldJobs = new List<WorkJobExecutionRequest>();

        // Track active jobs (for stats) without locking a List
        private readonly ConcurrentDictionary<int, WorkJobExecutionRequest> ActiveJobs = new ConcurrentDictionary<int, WorkJobExecutionRequest>();
        public WorkJobExecutionStat[] ActiveTaskStat() =>
            ActiveJobs.Values.Select(x => new WorkJobExecutionStat(x)).ToArray();

        public int MaxParallelTasks { get; set; } = 0;
        public static int DefaultSleepInterval = 25;

        public bool IsClosed { get; private set; } = false;
        public bool IsRunning { get; private set; } = false;
        public bool Active { get; private set; } = false;

        private CancellationTokenSource _runCts;
        private SemaphoreSlim _dispatchSignal = new SemaphoreSlim(0);
        private Task _dispatcherTask;

        private DateTime WentIdle = DateTime.UtcNow;

        // Metrics
        private int _cancelledInternal;
        private int _workDoneInternal;
        private int _inQueueInternal;
        private int _executingInternal;
        private int _totalWorkInternal;
        private int _reservedSlots;

        public int TotalWork => _totalWorkInternal;
        public int Executing => _executingInternal;
        public int InQueue => _inQueueInternal;
        public int WorkDone => _workDoneInternal;
        public int Cancelled => _cancelledInternal;

        public decimal TotalTaskResolutionTime { get; private set; } = 0m;
        public decimal AverageTaskResolutionTime => WorkDone > 0 ? TotalTaskResolutionTime / WorkDone : 0m;

        public TimeSpan TimeIdle => WentIdle > DateTime.UtcNow ? TimeSpan.Zero : DateTime.UtcNow - WentIdle;

        public WorkQueuer(string name, int maxThreads = -1, bool init_started = true) {
            if (maxThreads <= 0) maxThreads = Math.Max(2, Environment.ProcessorCount - 1);
            MaxParallelTasks = Math.Max(1, maxThreads);
            Name = name;

            if (init_started) Start();
        }

        public void Close() => IsClosed = true;

        public void Start() {
            if (IsRunning) return;

            Active = true;
            IsRunning = true;
            _runCts?.Dispose();
            _runCts = new CancellationTokenSource();

            _dispatcherTask = Task.Run(() => DispatchLoop(_runCts.Token));
            
            FlushHeldJobsToQueue();
            SignalDispatcher();
        }

        public async Task Stop(bool wait = true) {
            if (!IsRunning) return;

            // Prevent new jobs being written into the channel
            Active = false;

            if (wait) {
                // Wait for drain: no queued items and no active items
                while (Volatile.Read(ref _inQueueInternal) > 0 || !ActiveJobs.IsEmpty) {
                    await Task.Delay(100).ConfigureAwait(false);
                }
            } else {
                while (!ActiveJobs.IsEmpty) {
                    await Task.Delay(50).ConfigureAwait(false);
                }
            }

            // Cancel workers so WaitToReadAsync unblocks and loops can exit
            _runCts.Cancel();

            if (_dispatcherTask != null) {
                try {
                    await _dispatcherTask.ConfigureAwait(false);
                } catch (OperationCanceledException) {
                    // expected during shutdown
                }
            }

            IsRunning = false;
            _dispatcherTask = null;
        }

        private void FlushHeldJobsToQueue() {
            List<WorkJobExecutionRequest> pending = null;
            lock (HeldJobs) {
                if (HeldJobs.Count == 0) return;
                pending = new List<WorkJobExecutionRequest>(HeldJobs);
                HeldJobs.Clear();
            }

            foreach (var job in pending) {
                ScheduleActiveJob(job, alreadyCounted: false);
            }
        }

        private void ScheduleActiveJob(WorkJobExecutionRequest job, bool alreadyCounted) {
            if (!alreadyCounted) {
                Interlocked.Increment(ref _inQueueInternal);
            }

            bool startImmediately = false;
            lock (_overflowLock) {
                if (_overflowQueue.Count == 0 && CanStartNewJobUnsafe()) {
                    Interlocked.Increment(ref _reservedSlots);
                    startImmediately = true;
                } else {
                    _overflowQueue.Enqueue(job);
                }
            }

            if (startImmediately) {
                StartJob(job, reservedSlot: true);
            } else {
                SignalDispatcher();
            }
        }

        private void SignalDispatcher() {
            if (_dispatchSignal == null) return;
            try {
                _dispatchSignal.Release();
            } catch (SemaphoreFullException) {
                // ignore spuriously high release counts
            }
        }

        private async Task DispatchLoop(CancellationToken ct) {
            try {
                while (true) {
                    await _dispatchSignal.WaitAsync(ct).ConfigureAwait(false);

                    while (true) {
                        WorkJobExecutionRequest next = null;
                        lock (_overflowLock) {
                            if (_overflowQueue.Count == 0) break;
                            if (!CanStartNewJobUnsafe()) break;
                            next = _overflowQueue.Dequeue();
                            Interlocked.Increment(ref _reservedSlots);
                        }

                        if (next == null) break;
                        StartJob(next, reservedSlot: true);
                    }
                }
            } catch (OperationCanceledException) {
                // graceful shutdown
            }
        }

        private void StartJob(WorkJobExecutionRequest job, bool reservedSlot) {
            Interlocked.Decrement(ref _inQueueInternal);

            if (reservedSlot) {
                Interlocked.Decrement(ref _reservedSlots);
            }

            if (job.Cancellation.IsCancellationRequested) {
                Interlocked.Increment(ref _cancelledInternal);
                Interlocked.Increment(ref _workDoneInternal);
                job.TaskCompletionSource.TrySetCanceled(job.Cancellation.Token);
                job.Cancellation.Dispose();
                SignalDispatcher();
                return;
            }

            WentIdle = DateTime.UtcNow;

            ActiveJobs.TryAdd(job.id, job);
            Interlocked.Increment(ref _executingInternal);

            var workerId = unchecked(Environment.TickCount ^ job.id);

            var executionTask = Task.Run(async () => {
                try {
                    await ExecuteJob(job, workerId).ConfigureAwait(false);
                } catch (Exception ex) {
                    if (Debugger.IsAttached) {
                        Debugger.Break();
                    }
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} terminated unexpectedly: {ex.Message}");
                } finally {
                    ActiveJobs.TryRemove(job.id, out _);
                    Interlocked.Decrement(ref _executingInternal);
                    SignalDispatcher();
                }
            });

            _ = executionTask.ContinueWith(t => {
                if (t.IsFaulted) {
                    _ = t.Exception;
                }
            }, TaskScheduler.Default);
        }

        private bool CanStartNewJobUnsafe() {
            if (_runCts == null) return false;
            if (_runCts.IsCancellationRequested) return false;

            var running = Volatile.Read(ref _executingInternal);
            var reserved = Volatile.Read(ref _reservedSlots);
            return running + reserved < EffectiveParallelLimit();
        }

        private int EffectiveParallelLimit() => Math.Min(500, Math.Max(MaxParallelTasks, 1));

        public async Task AccompanyJob(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var wj = EnqueueTask(a, exceptionHandler, finished);
            await wj;
        }

        public WorkJobExecutionRequest Enqueue(WorkJob job, CancellationToken? requestCancellation = null) {
            var request = new WorkJobExecutionRequest(job, requestCancellation) {
                EnqueuedTime = DateTime.UtcNow,
                Status = WorkJobRequestStatus.Queued,
                WorkQueuer = this
            };
            if (FiTechCoreExtensions.DebugTasks) {
                request.StackTrace = new StackTrace();
            }
            if (job.Name is null && Debugger.IsAttached) {
                Debugger.Break();
            }

            Interlocked.Increment(ref _totalWorkInternal);

            if (Active) {
                ScheduleActiveJob(request, alreadyCounted: false);
            } else {
                lock (HeldJobs) {
                    HeldJobs.Add(request);
                }
            }

            _ = SafeInvoke(OnWorkEnqueued, request);
            return request;
        }

        public void Enqueue(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished) { Name = "Annonymous Work Item" };
            _ = Enqueue(retv);
        }
        public WorkJobExecutionRequest EnqueueTask(Func<CancellationToken, ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            return Enqueue(retv);
        }
        public WorkJobExecutionRequest EnqueueTask(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            return Enqueue(retv);
        }

        private async Task ExecuteJob(WorkJobExecutionRequest job, int workerId) {
            job.TimeInQueue = DateTime.UtcNow - (job.EnqueuedTime ?? DateTime.UtcNow);
            await SafeInvoke(OnWorkDequeued, job).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            Exception terminalException = null;

            try {
                // Telemetry
                if (job.WorkJob.AllowTelemetry) {
                    job.LoggingActivity = Fi.Tech.CreateTelemetryActivity(job.WorkJob?.Name ?? "Unnamed Task", ActivityKind.Internal);
                    if (job.LoggingActivity != null) {
                        foreach (var kv in DefaultLoggingTags) job.LoggingActivity.AddTag(kv.Key, kv.Value);
                        foreach (var kv in job.WorkJob.AdditionalTelemetryTags) job.LoggingActivity.AddTag(kv.Key, kv.Value);
                        job.LoggingActivity.AddTag("WorkQueuer", Name);
                        job.LoggingActivity.Start();
                        job.LoggingActivity.SetStartTime(DateTime.UtcNow);
                    }
                }

                job.DequeuedTime = DateTime.UtcNow;
                job._tcsNotifyDequeued.TrySetResult(0);
                job.Status = WorkJobRequestStatus.Running;

                if (job.WorkJob.action != null) {
                    using (var ctsLinked = CancellationTokenSource.CreateLinkedTokenSource(job.Cancellation.Token, job.RequestCancellation)) {
                        job.WorkJob.ActionTask = job.WorkJob.action(ctsLinked.Token);
                    }
                    await job.WorkJob.ActionTask.ConfigureAwait(false);
                    job.Status = WorkJobRequestStatus.Finished;
                    job.LoggingActivity?.SetStatus(ActivityStatusCode.Ok);
                }

                if (job.WorkJob.finished != null) {
                    try {
                        await job.WorkJob.finished(true).ConfigureAwait(false);
                    } catch (Exception ex) {
                        var wrapped = new WorkJobException("Error Executing WorkJob", job, ex);
                        Fi.Tech.Throw(wrapped);
                    }
                }

                Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} executed OK");
            } catch (Exception execEx) {
                job.Status = WorkJobRequestStatus.Failed;
                job.LoggingActivity?.AddTag("Exception",
                    JsonConvert.SerializeObject(ExceptionExtensions.ToRecursiveInnerExceptions(execEx)));
                job.LoggingActivity?.SetStatus(ActivityStatusCode.Error);

                var wrapped = new WorkJobException("Error Executing WorkJob", job, execEx);
                Exception handlerEx = null;

                if (job.WorkJob.handling != null) {
                    try {
                        await job.WorkJob.handling(wrapped).ConfigureAwait(false);
                    } catch (Exception ex) {
                        handlerEx = ex;
                        try {
                            var t = OnExceptionInHandler?.Invoke(job, execEx, ex);
                            if (t is Task) await t.ConfigureAwait(false);
                        } catch (Exception exx) {
                            Fi.Tech.Throw(new AggregateException("User code generated exception in the handler AND in the handler of the handler.", execEx, ex, exx));
                        }
                    }
                } else {
                    Fi.Tech.Throw(wrapped);
                }

                if (job.WorkJob.finished != null) {
                    try {
                        await job.WorkJob.finished(false).ConfigureAwait(false);
                    } catch (Exception ex2) {
                        var wrapped2 = new WorkJobException("Error Executing WorkJob", job, new AggregateException(execEx, ex2));
                        Fi.Tech.Throw(wrapped2);
                    }
                }

                terminalException = handlerEx != null
                    ? new AggregateException("Job failed and handler threw", execEx, handlerEx)
                    : execEx;

                Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} thrown an Exception: {execEx.Message}");
            } finally {
                try {
                    sw.Stop();
                    job.CompletedTime = DateTime.UtcNow;
                    job.TimeToComplete = sw.Elapsed;

                    // Only set Finished if we didn't fail earlier
                    if (job.Status != WorkJobRequestStatus.Failed) {
                        job.Status = WorkJobRequestStatus.Finished;
                    }

                    try {
                        await SafeInvoke(OnWorkComplete, job).ConfigureAwait(false);
                    } catch (Exception x) {
                        Fi.Tech.Throw(x);
                    }

                    Interlocked.Increment(ref _workDoneInternal);
                    if (job.Cancellation.IsCancellationRequested) {
                        Interlocked.Increment(ref _cancelledInternal);
                    }
                    job.Cancellation.Dispose();

                    job.LoggingActivity?.SetEndTime(DateTime.UtcNow);
                    job.LoggingActivity?.Dispose();

                    if (terminalException != null) {
                        job.TaskCompletionSource.TrySetException(terminalException);
                        _ = job.TaskCompletionSource.Task.Exception; // observe
                    } else {
                        job.TaskCompletionSource.TrySetResult(0);
                    }

                    TotalTaskResolutionTime += (decimal)sw.Elapsed.TotalMilliseconds;
                    WentIdle = DateTime.UtcNow;
                } catch (Exception cleanupEx) {
                    if (Debugger.IsAttached) Debugger.Break();
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker cleanup error: {cleanupEx.Message}");
                }
                Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} cleanup OK");
            }
        }

        private static async Task SafeInvoke(Func<WorkJobExecutionRequest, Task> ev, WorkJobExecutionRequest r) {
            if (ev == null) return;
            try { await ev(r).ConfigureAwait(false); } catch { /* swallow user event exceptions, already surfaced via Fi.Tech.Throw elsewhere if needed */ }
        }
        private static async Task SafeInvoke(Func<WorkJobExecutionRequest, Exception, Exception, Task> ev, WorkJobExecutionRequest r, Exception a, Exception b) {
            if (ev == null) return;
            try { await ev(r, a, b).ConfigureAwait(false); } catch { /* swallow */ }
        }

        public static async Task Live(Action<WorkQueuer> act, int parallelSize = -1) {
            if (parallelSize <= 0) parallelSize = Environment.ProcessorCount;
            await using (var queuer = new WorkQueuer($"AnnonymousLiveQueuer", parallelSize)) {
                queuer.Start();
                act(queuer);
                await queuer.Stop(true).ConfigureAwait(false);
            }
        }

        public void Dispose() {
            Stop(true).GetAwaiter().GetResult();
            _runCts?.Dispose();
            _dispatchSignal?.Dispose();
            _dispatchSignal = null;
        }
        public async ValueTask DisposeAsync() {
            await Stop(true).ConfigureAwait(false);
            _runCts?.Dispose();
            _dispatchSignal?.Dispose();
            _dispatchSignal = null;
        }
    }
}
