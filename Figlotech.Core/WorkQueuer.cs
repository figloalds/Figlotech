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

    public sealed class ScheduledTaskOptions {
        public TimeSpan? RecurrenceInterval { get; set; }
        public bool FireIfMissed { get; set; } = false;
        public DateTime? ScheduledTime { get; set; }
        public CancellationToken? CancellationToken { get; set; }
    }

    public sealed class ScheduleInfo {
        public string Identifier { get; set; }
        public DateTime Created { get; set; }
        public DateTime NextScheduledTime { get; set; }
        public TimeSpan? RecurrenceInterval { get; set; }
        public bool FireIfMissed { get; set; }
        public bool IsExecuting { get; set; }
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
                Cancellation?.Dispose();
                LoggingActivity?.Dispose();
                _disposed = true;
            }
        }
        ~WorkJobExecutionRequest() {
            _disposed = true;
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

        // Use long for thread-safe DateTime storage (DateTime.Ticks)
        private long _wentIdleTicks = DateTime.UtcNow.Ticks;
        public DateTime WentIdle => new DateTime(Interlocked.Read(ref _wentIdleTicks), DateTimeKind.Utc);

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

        // Use long for thread-safe atomic operations (stores milliseconds as ticks)
        private long _totalTaskResolutionTimeTicks;
        public TimeSpan TotalTaskResolutionTime => TimeSpan.FromTicks(Interlocked.Read(ref _totalTaskResolutionTimeTicks));
        public TimeSpan AverageTaskResolutionTime => WorkDone > 0 ? TimeSpan.FromTicks(Interlocked.Read(ref _totalTaskResolutionTimeTicks) / WorkDone) : TimeSpan.Zero;

        public TimeSpan TimeIdle => WentIdle > DateTime.UtcNow ? TimeSpan.Zero : DateTime.UtcNow - WentIdle;

        // Scheduling infrastructure
        private readonly Dictionary<string, ScheduledTaskEntry> _scheduledTasks = new Dictionary<string, ScheduledTaskEntry>();
        private readonly object _scheduledTasksLock = new object();
        private readonly List<ScheduledTaskEntry> _missedSchedules = new List<ScheduledTaskEntry>();

        private sealed class ScheduledTaskEntry {
            public string Identifier { get; set; }
            public DateTime Created { get; set; }
            public WorkJob Job { get; set; }
            public ScheduledTaskOptions Options { get; set; }
            public Timer Timer { get; set; }
            public CancellationTokenSource Cancellation { get; set; }
            public DateTime ScheduledTime { get; set; }
            public bool IsExecuting { get; set; }
        }

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
            ProcessMissedSchedules();
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
            } catch (ObjectDisposedException) {
                // semaphore was disposed, ignore
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

            Interlocked.Exchange(ref _wentIdleTicks, DateTime.UtcNow.Ticks);

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

        // Maximum absolute parallel limit to prevent resource exhaustion
        public static int AbsoluteMaxParallelLimit { get; set; } = 500;

        private int EffectiveParallelLimit() => Math.Min(AbsoluteMaxParallelLimit, Math.Max(MaxParallelTasks, 1));

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
                    CancellationTokenSource ctsLinked = null;
                    try {
                        ctsLinked = CancellationTokenSource.CreateLinkedTokenSource(job.Cancellation.Token, job.RequestCancellation);
                        job.WorkJob.ActionTask = job.WorkJob.action(ctsLinked.Token);
                        await job.WorkJob.ActionTask.ConfigureAwait(false);
                    } finally {
                        ctsLinked?.Dispose();
                    }
                    job.Status = WorkJobRequestStatus.Finished;
                    job.LoggingActivity?.SetStatus(ActivityStatusCode.Ok);
                }

                if (job.WorkJob.finished != null) {
                    try {
                        await job.WorkJob.finished(true).ConfigureAwait(false);
                    } catch (Exception ex) {
                        var wrapped = new WorkJobException("Error Executing WorkJob", job, ex);
                        Fi.Tech.SwallowException(wrapped);
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
                            // Capture the exception for TCS instead of throwing
                            terminalException = new AggregateException("User code generated exception in the handler AND in the handler of the handler.", execEx, ex, exx);
                            try {
                                Fi.Tech.SwallowException(new AggregateException("User code generated exception in the handler AND in the handler of the handler.", execEx, ex, exx));
                            } catch { }
                        }
                    }
                } else {
                    // Capture the exception for TCS instead of throwing
                    terminalException = wrapped;
                    try {
                        Fi.Tech.SwallowException(wrapped);
                    } catch { }
                }

                // Only call finished callback if we haven't already set terminalException
                if (job.WorkJob.finished != null && terminalException == null) {
                    try {
                        await job.WorkJob.finished(false).ConfigureAwait(false);
                    } catch (Exception ex2) {
                        var wrapped2 = new WorkJobException("Error Executing WorkJob", job, new AggregateException(execEx, ex2));
                        terminalException = wrapped2;
                        try {
                            Fi.Tech.SwallowException(wrapped2);
                        } catch { }
                    }
                }

                // Set terminalException if not already set
                if (terminalException == null) {
                    terminalException = handlerEx != null
                        ? new AggregateException("Job failed and handler threw", execEx, handlerEx)
                        : execEx;
                }

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

                    await SafeInvoke(OnWorkComplete, job).ConfigureAwait(false);

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

                    Interlocked.Add(ref _totalTaskResolutionTimeTicks, sw.Elapsed.Ticks);
                    Interlocked.Exchange(ref _wentIdleTicks, DateTime.UtcNow.Ticks);
                } catch (Exception cleanupEx) {
                    if (Debugger.IsAttached) Debugger.Break();
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker cleanup error: {cleanupEx.Message}");
                }
                Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} cleanup OK");
            }
        }

        private static async Task SafeInvoke(Func<WorkJobExecutionRequest, Task> ev, WorkJobExecutionRequest r) {
            if (ev == null) return;
            try {
                await ev(r).ConfigureAwait(false);
            } catch (Exception ex) {
                Fi.Tech.SwallowException(ex);
            }
        }

        private static async Task SafeInvoke(Func<WorkJobExecutionRequest, Exception, Exception, Task> ev, WorkJobExecutionRequest r, Exception a, Exception b) {
            if (ev == null) return;
            try {
                await ev(r, a, b).ConfigureAwait(false);
            } catch (Exception ex) {
                Fi.Tech.SwallowException(ex);
            }
        }

        public static async Task Live(Action<WorkQueuer> act, int parallelSize = -1) {
            if (parallelSize <= 0) parallelSize = Environment.ProcessorCount;
            await using (var queuer = new WorkQueuer($"AnnonymousLiveQueuer", parallelSize)) {
                queuer.Start();
                act(queuer);
                await queuer.Stop(true).ConfigureAwait(false);
            }
        }

        #region Scheduling

        public void ScheduleTask(string identifier, WorkJob job, ScheduledTaskOptions options) {
            if (string.IsNullOrEmpty(identifier)) throw new ArgumentNullException(nameof(identifier));
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (options == null) throw new ArgumentNullException(nameof(options));

            lock (_scheduledTasksLock) {
                // Unschedule existing if present
                UnscheduleInternal(identifier);

                var scheduledTime = options.ScheduledTime ?? DateTime.UtcNow;
                var cts = CancellationTokenSource.CreateLinkedTokenSource(
                    options.CancellationToken ?? CancellationToken.None
                );

                var entry = new ScheduledTaskEntry {
                    Identifier = identifier,
                    Created = DateTime.UtcNow,
                    Job = job,
                    Options = options,
                    Cancellation = cts,
                    ScheduledTime = scheduledTime
                };

                _scheduledTasks[identifier] = entry;
                SetupTimer(entry);
            }
        }

        private void SetupTimer(ScheduledTaskEntry entry) {
            // Dispose existing timer before creating new one
            entry.Timer?.Dispose();
            
            var now = DateTime.UtcNow;
            var delay = entry.ScheduledTime - now;
            var delayMs = Math.Max(0, (long)delay.TotalMilliseconds);

            // Cap timer at 1 minute to handle clock changes and long waits
            const int maxTimerIntervalMs = 60000;
            var timerDelay = (int)Math.Min(delayMs, maxTimerIntervalMs);

            entry.Timer = new Timer(
                state => OnTimerFired((ScheduledTaskEntry)state),
                entry,
                timerDelay,
                Timeout.Infinite
            );
        }

        private void OnTimerFired(ScheduledTaskEntry entry) {
            if (entry.Cancellation.IsCancellationRequested) {
                CleanupSchedule(entry);
                return;
            }

            var now = DateTime.UtcNow;
            var timeUntilScheduled = entry.ScheduledTime - now;
            const int timerPrecisionMs = 50;

            // If not yet time (within precision window), reschedule
            if (timeUntilScheduled.TotalMilliseconds > timerPrecisionMs) {
                SetupTimer(entry);
                return;
            }

            // Check if WorkQueuer is active
            if (!IsRunning || !Active) {
                if (entry.Options.FireIfMissed) {
                    lock (_scheduledTasksLock) {
                        _missedSchedules.Add(entry);
                    }
                }
                // For recurring tasks, calculate next occurrence
                if (entry.Options.RecurrenceInterval.HasValue) {
                    RescheduleEntry(entry);
                } else {
                    CleanupSchedule(entry);
                }
                return;
            }

            // Execute the job
            ExecuteScheduledJob(entry);
        }

        private void ExecuteScheduledJob(ScheduledTaskEntry entry) {
            lock (_scheduledTasksLock) {
                if (entry.IsExecuting) {
                    // Already running, skip this occurrence
                    if (entry.Options.RecurrenceInterval.HasValue) {
                        RescheduleEntry(entry);
                    }
                    return;
                }
                entry.IsExecuting = true;
            }

            // Dispose timer to prevent duplicate firing
            entry.Timer?.Dispose();
            entry.Timer = null;

            var request = Enqueue(entry.Job, entry.Cancellation.Token);
            
            request.GetAwaiter().OnCompleted(() => {
                lock (_scheduledTasksLock) {
                    entry.IsExecuting = false;

                    if (entry.Options.RecurrenceInterval.HasValue && !entry.Cancellation.IsCancellationRequested) {
                        RescheduleEntry(entry);
                    } else {
                        CleanupSchedule(entry);
                    }
                }
            });
        }

        private void RescheduleEntry(ScheduledTaskEntry entry) {
            if (!entry.Options.RecurrenceInterval.HasValue) return;

            var now = DateTime.UtcNow;
            var nextRun = entry.ScheduledTime;
            var interval = entry.Options.RecurrenceInterval.Value;

            // Calculate next occurrence using loop to prevent drift
            do {
                nextRun = nextRun.Add(interval);
            } while (nextRun <= now);

            entry.ScheduledTime = nextRun;
            SetupTimer(entry);
        }

        private void CleanupSchedule(ScheduledTaskEntry entry) {
            lock (_scheduledTasksLock) {
                entry.Timer?.Dispose();
                entry.Cancellation?.Dispose();
                _scheduledTasks.Remove(entry.Identifier);
            }
        }

        private void UnscheduleInternal(string identifier) {
            if (_scheduledTasks.TryGetValue(identifier, out var entry)) {
                entry.Cancellation?.Cancel();
                entry.Timer?.Dispose();
                entry.Cancellation?.Dispose();
                _scheduledTasks.Remove(identifier);
            }
        }

        public void Unschedule(string identifier) {
            lock (_scheduledTasksLock) {
                UnscheduleInternal(identifier);
            }
        }

        public bool IsScheduled(string identifier) {
            lock (_scheduledTasksLock) {
                return _scheduledTasks.ContainsKey(identifier);
            }
        }

        public string[] GetScheduledIdentifiers() {
            lock (_scheduledTasksLock) {
                return _scheduledTasks.Keys.ToArray();
            }
        }

        public ScheduleInfo GetScheduleInfo(string identifier) {
            lock (_scheduledTasksLock) {
                if (!_scheduledTasks.TryGetValue(identifier, out var entry)) {
                    return null;
                }
                return new ScheduleInfo {
                    Identifier = entry.Identifier,
                    Created = entry.Created,
                    NextScheduledTime = entry.ScheduledTime,
                    RecurrenceInterval = entry.Options.RecurrenceInterval,
                    FireIfMissed = entry.Options.FireIfMissed,
                    IsExecuting = entry.IsExecuting
                };
            }
        }

        public ScheduleInfo[] GetAllScheduleInfo() {
            lock (_scheduledTasksLock) {
                return _scheduledTasks.Values.Select(entry => new ScheduleInfo {
                    Identifier = entry.Identifier,
                    Created = entry.Created,
                    NextScheduledTime = entry.ScheduledTime,
                    RecurrenceInterval = entry.Options.RecurrenceInterval,
                    FireIfMissed = entry.Options.FireIfMissed,
                    IsExecuting = entry.IsExecuting
                }).ToArray();
            }
        }

        private void ProcessMissedSchedules() {
            List<ScheduledTaskEntry> missed;
            lock (_scheduledTasksLock) {
                missed = new List<ScheduledTaskEntry>(_missedSchedules);
                _missedSchedules.Clear();
            }

            foreach (var entry in missed) {
                if (entry.Options.FireIfMissed && !entry.Cancellation.IsCancellationRequested) {
                    ExecuteScheduledJob(entry);
                }
            }
        }

        #endregion

        private void DisposeScheduledTasks() {
            lock (_scheduledTasksLock) {
                foreach (var entry in _scheduledTasks.Values) {
                    entry.Cancellation?.Cancel();
                    entry.Timer?.Dispose();
                    entry.Cancellation?.Dispose();
                }
                _scheduledTasks.Clear();
                _missedSchedules.Clear();
            }
        }

        public void Dispose() {
            // Use a synchronous wait with timeout to avoid deadlocks
            // when Dispose() is called from a sync context
            try {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30))) {
                    Stop(true).Wait(cts.Token);
                }
            } catch (OperationCanceledException) {
                // Timeout - force shutdown
            } catch (Exception) {
                // Ignore other exceptions during dispose
            }
            DisposeScheduledTasks();
            _runCts?.Dispose();
            _dispatchSignal?.Dispose();
            _dispatchSignal = null;
        }
        public async ValueTask DisposeAsync() {
            await Stop(true).ConfigureAwait(false);
            DisposeScheduledTasks();
            _runCts?.Dispose();
            _dispatchSignal?.Dispose();
            _dispatchSignal = null;
        }
    }
}
