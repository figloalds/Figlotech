using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
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
        public readonly int id = Interlocked.Increment(ref idGen);
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
        public TaskCompletionSource<int> TaskCompletionSource => _taskCompletionSource;

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
    }

    public sealed class WorkJob {
        public Func<CancellationToken, ValueTask> action;
        public Func<bool, ValueTask> finished;
        public Func<Exception, ValueTask> handling;

        static readonly Func<Func<ValueTask>, Func<CancellationToken, ValueTask>> ConvertActionFromAbsentOptionalParameter
            = fn => (ignore) => fn();

        public String Name { get; set; } = null;
        public String Description { get; set; } = null;
        public bool AllowTelemetry { get; set; } = true;

        internal Dictionary<string, object> _additionalTelemetryTags;
        public Dictionary<string, object> AdditionalTelemetryTags {
            get => _additionalTelemetryTags ??= new Dictionary<string, object>();
            private set => _additionalTelemetryTags = value;
        }

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

    }

    public sealed class WorkJobExecutionStat {
        public string Description { get; set; }
        public DateTime? EnqueuedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public decimal TimeWaiting { get; set; }
        public decimal TimeInExecution { get; set; }
        public Dictionary<string, object> AdditionalTelemetryTags { get; set; }
        public StackTrace SchedulingContextStackTrace { get; set; }
        public CancellationTokenSource CancellationTokenSource { get; set;  }
        public WorkJobExecutionStat(WorkJobExecutionRequest x) {
            Description = x.WorkJob.Name;
            EnqueuedAt = x.EnqueuedTime;
            StartedAt = x.DequeuedTime;
            TimeWaiting = (decimal)((x.DequeuedTime ?? DateTime.UtcNow) - (x.EnqueuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
            TimeInExecution = (decimal)(DateTime.UtcNow - (x.DequeuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
            SchedulingContextStackTrace = x?.StackTrace;
            AdditionalTelemetryTags = x.WorkJob.AdditionalTelemetryTags;
            CancellationTokenSource = x.Cancellation;
        }
    }


    public sealed class WorkQueuer : IDisposable, IAsyncDisposable {
        public static int qid_increment = 0;
        private readonly int __qid = ++qid_increment;
        public int QID => __qid;
        public string Name { get; set; }

        public event Func<WorkJobExecutionRequest, Task> OnWorkEnqueued;
        public event Func<WorkJobExecutionRequest, Task> OnWorkDequeued;
        public event Func<WorkJobExecutionRequest, Task> OnWorkComplete;
        public event Func<WorkJobExecutionRequest, Exception, Exception, Task> OnExceptionInHandler;

        private Dictionary<string, object> _defaultLoggingTags;
        public Dictionary<string, object> DefaultLoggingTags => _defaultLoggingTags ??= new Dictionary<string, object>();

        // Held when not Active, flushed on Start()
        private readonly ConcurrentQueue<WorkJobExecutionRequest> HeldJobs = new ConcurrentQueue<WorkJobExecutionRequest>();

        // Track active jobs (for stats) without locking a List
        private readonly ConcurrentDictionary<int, WorkJobExecutionRequest> ActiveJobs = new ConcurrentDictionary<int, WorkJobExecutionRequest>();
        public WorkJobExecutionStat[] ActiveTaskStat() =>
            ActiveJobs.Values.Select(x => new WorkJobExecutionStat(x)).ToArray();

        public int MaxParallelTasks { get; set; } = 0;

        // Cached effective parallel limit; invalidated when MaxParallelTasks or AbsoluteMaxParallelLimit changes
        private int _cachedEffectiveParallelLimit;
        private int _cachedMaxParallelTasksForLimit;
        private int _cachedAbsoluteMaxParallelLimitForLimit;
        public static int DefaultSleepInterval = 25;

        // Volatile-backed state flags for cross-thread visibility
        private volatile bool _isClosed;
        private volatile bool _isRunning;
        private volatile bool _active;

        public bool IsClosed { get => _isClosed; private set => _isClosed = value; }
        public bool IsRunning { get => _isRunning; private set => _isRunning = value; }
        public bool Active { get => _active; private set => _active = value; }

        private Channel<WorkJobExecutionRequest> _workChannel;
        private readonly object _channelLock = new object();
        public int ChannelCapacity { get; set; } = 0;
        private CancellationTokenSource _runCts;
        private readonly object _workersLock = new object();
        private readonly List<Task> _workerTasks = new List<Task>();
        // Intentionally not disposed: public Stop/Enqueue calls can still be waiting on it; disposal could fault them, and GC will reclaim it.
        private readonly SemaphoreSlim _lifecycleLock = new SemaphoreSlim(1, 1);
        private volatile bool _drainOnStop;
        private int _numberOfActualWorkers;
        private int _nextWorkerId;
        private int _disposed;

        // Signaled when all queued and active work has drained (used by Stop)
        private volatile TaskCompletionSource<bool> _drainTcs;
        private volatile TaskCompletionSource<bool> _stopTcs;

        // Use long for thread-safe DateTime storage (DateTime.Ticks)
        private long _wentIdleTicks = DateTime.UtcNow.Ticks;
        public DateTime WentIdle => new DateTime(Interlocked.Read(ref _wentIdleTicks), DateTimeKind.Utc);

        // Metrics
        private int _cancelledInternal;
        private int _workDoneInternal;
        private int _inQueueInternal;
        private int _executingInternal;
        private int _totalWorkInternal;

        public int TotalWork => _totalWorkInternal;
        public int Executing => _executingInternal;
        public int InQueue => _inQueueInternal;
        public int WorkDone => _workDoneInternal;
        public int Cancelled => _cancelledInternal;
        public int NumberOfActualWorkers => Volatile.Read(ref _numberOfActualWorkers);

        // Use long for thread-safe atomic operations (stores milliseconds as ticks)
        private long _totalTaskResolutionTimeTicks;
        public TimeSpan TotalTaskResolutionTime => TimeSpan.FromTicks(Interlocked.Read(ref _totalTaskResolutionTimeTicks));
        public TimeSpan AverageTaskResolutionTime {
            get {
                var done = Volatile.Read(ref _workDoneInternal);
                return done > 0 ? TimeSpan.FromTicks(Interlocked.Read(ref _totalTaskResolutionTimeTicks) / done) : TimeSpan.Zero;
            }
        }

        // Cached Stopwatch-to-TimeSpan conversion ratio (avoids recomputing in hot path)
        private static readonly double StopwatchTickToTimeSpanTicks = (double)TimeSpan.TicksPerSecond / Stopwatch.Frequency;

        // Worker scaling rate limiting
        private long _lastWorkerScaleTicks = DateTime.UtcNow.Ticks;
        private const int MinScaleIntervalMs = 100;

        public TimeSpan TimeIdle => WentIdle > DateTime.UtcNow ? TimeSpan.Zero : DateTime.UtcNow - WentIdle;

        // Scheduling infrastructure - consolidated single timer with priority queue
        private readonly Dictionary<string, ScheduledTaskEntry> _scheduledTasks = new Dictionary<string, ScheduledTaskEntry>();
        private readonly SortedSet<ScheduledTaskEntry> _scheduledTaskQueue = new SortedSet<ScheduledTaskEntry>();
        private readonly object _scheduledTasksLock = new object();
        private readonly List<ScheduledTaskEntry> _missedSchedules = new List<ScheduledTaskEntry>();
        private Timer _consolidatedTimer;

        private sealed class ScheduledTaskEntry : IComparable<ScheduledTaskEntry> {
            public string Identifier { get; set; }
            public DateTime Created { get; set; }
            public WorkJob Job { get; set; }
            public ScheduledTaskOptions Options { get; set; }
            public CancellationTokenSource Cancellation { get; set; }
            public DateTime ScheduledTime { get; set; }
            public bool IsExecuting { get; set; }

            public int CompareTo(ScheduledTaskEntry other) {
                if (other == null) return 1;
                var timeCompare = ScheduledTime.CompareTo(other.ScheduledTime);
                if (timeCompare != 0) return timeCompare;
                // Tie-breaker to ensure uniqueness in SortedSet
                return string.Compare(Identifier, other.Identifier, StringComparison.Ordinal);
            }
        }

        public WorkQueuer(string name, int maxThreads = -1, bool init_started = true) {
            if (maxThreads <= 0) maxThreads = Math.Max(2, Environment.ProcessorCount - 1);
            MaxParallelTasks = Math.Max(1, maxThreads);
            Name = name;

            if (init_started) Start();
        }

        public void Close() => IsClosed = true;

        public void Start() {
            List<ScheduledTaskEntry> missedSchedules = null;
            _lifecycleLock.Wait();
            try {
                if (IsRunning) return;

                Active = true;
                IsRunning = true;
                _stopTcs = null;
                _drainOnStop = false;
                _runCts?.Dispose();
                _runCts = new CancellationTokenSource();

                EnsureMinimumWorkers();

                FlushHeldJobsToQueue();
                missedSchedules = TakeMissedSchedules();
                EnsureWorkerCapacityForDemand();
            } finally {
                _lifecycleLock.Release();
            }
            RunMissedSchedules(missedSchedules);
        }

        /// <summary>
        /// Stops the queuer exactly once for all concurrent callers. When <paramref name="wait"/> is true,
        /// queued and active work is drained; when false, queued work is cancelled and only active work is awaited.
        /// Work accepted while the queuer is inactive and still held when Stop begins is terminally faulted or cancelled,
        /// for either value of <paramref name="wait"/>; work that Start has already flushed to the queue is drained
        /// when <paramref name="wait"/> is true or cancelled when it is false.
        /// </summary>
        public async Task Stop(bool wait = true) {
            Task stopTask;
            await _lifecycleLock.WaitAsync().ConfigureAwait(false);
            try {
                if (_stopTcs != null) {
                    stopTask = _stopTcs.Task;
                } else if (!IsRunning && HeldJobs.IsEmpty) {
                    return;
                } else {
                    var stopTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _stopTcs = stopTcs;
                    Active = false;
                    _drainOnStop = wait;
                    try {
                        _runCts?.Cancel();
                    } catch (ObjectDisposedException) {
                        // Dispose may have timed out and disposed the run CTS while Stop was in flight.
                    }
                    stopTask = StopCore(wait, stopTcs);
                }
            } finally {
                _lifecycleLock.Release();
            }

            await stopTask.ConfigureAwait(false);
        }

        private async Task StopCore(bool wait, TaskCompletionSource<bool> stopTcs) {
            try {
                FailHeldJobs(new OperationCanceledException($"WorkQueuer \"{Name}\" was stopped with work held."));
                if (!wait) {
                    DrainQueuedJobs();
                }
                await WaitForDrainAsync().ConfigureAwait(false);

                _drainOnStop = false;
                Task[] workers;
                lock (_workersLock) {
                    workers = _workerTasks.ToArray();
                }
                if (workers.Length > 0) {
                    try {
                        await Task.WhenAll(workers).ConfigureAwait(false);
                    } catch (OperationCanceledException) {
                        // expected during shutdown
                    }
                }

                DrainQueuedJobs();
                lock (_workersLock) {
                    _workerTasks.RemoveAll(task => task.IsCompleted);
                }
                IsRunning = false;
                stopTcs.TrySetResult(true);
            } catch (Exception ex) {
                IsRunning = false;
                stopTcs.TrySetException(ex);
            } finally {
                _drainOnStop = false;
            }
        }

        public Task WaitForIdleAsync() {
            // Idle is defined by the same condition Stop uses to drain: nothing queued
            // (neither counted nor still sitting in the channel) and no active jobs.
            // Awaiting the worker-loop tasks themselves would hang — workers are
            // long-lived and only exit on Stop/Dispose.
            return WaitForDrainAsync();
        }

        private async Task WaitForDrainAsync() {
            if (IsDrainComplete()) return;
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _drainTcs = tcs;
            SignalDrainIfComplete();
            await tcs.Task.ConfigureAwait(false);
            Interlocked.CompareExchange(ref _drainTcs, null, tcs);
        }

        private void FlushHeldJobsToQueue() {
            // Start holds _lifecycleLock throughout this flush, so Stop cannot drain the channel between
            // this write and its final drain. A held job is therefore either queued before Stop begins or
            // failed by Stop while still held.
            while (HeldJobs.TryDequeue(out var job)) {
                try {
                    WriteQueuedJob(job, alreadyCounted: false);
                } catch (Exception ex) {
                    FailRejectedRequest(job, ex);
                    throw;
                }
            }
        }

        private void FailHeldJobs(Exception exception) {
            while (HeldJobs.TryDequeue(out var job)) {
                FailRejectedRequest(job, exception);
                // Account held work the same way CancelOrphanedJob accounts drained queued work:
                // it was admitted (counted in TotalWork) and is now terminally resolved.
                Interlocked.Increment(ref _workDoneInternal);
                Interlocked.Increment(ref _cancelledInternal);
                try {
                    job.Dispose();
                } catch {
                }
            }
        }

        private void WriteQueuedJob(WorkJobExecutionRequest job, bool alreadyCounted) {
            if (!alreadyCounted) {
                Interlocked.Increment(ref _inQueueInternal);
            }

            try {
                if (!GetOrCreateChannel().Writer.TryWrite(job)) {
                    throw new InvalidOperationException($"Unable to queue work item on \"{Name}\".");
                }
            } catch {
                Interlocked.Decrement(ref _inQueueInternal);
                throw;
            }

            EnsureWorkerCapacityForDemand();
        }

        private async Task<bool> WriteQueuedJobAsync(WorkJobExecutionRequest job) {
            Channel<WorkJobExecutionRequest> channel;
            CancellationToken cancellationToken;

            await _lifecycleLock.WaitAsync().ConfigureAwait(false);
            try {
                if (IsClosed) {
                    FailRejectedRequest(job, new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed."));
                    return false;
                }
                if (_stopTcs != null) {
                    FailRejectedRequest(job, new InvalidOperationException($"WorkQueuer \"{Name}\" is stopping."));
                    return false;
                }

                channel = GetOrCreateChannel();
                cancellationToken = _runCts?.Token ?? CancellationToken.None;
                Interlocked.Increment(ref _inQueueInternal);
            } finally {
                _lifecycleLock.Release();
            }

            try {
                await channel.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false);
            } catch (OperationCanceledException ex) {
                Interlocked.Decrement(ref _inQueueInternal);
                FailRejectedRequest(job, new OperationCanceledException($"WorkQueuer \"{Name}\" stopped before the work item could be queued.", ex, cancellationToken));
                SignalDrainIfComplete();
                throw;
            }

            await _lifecycleLock.WaitAsync().ConfigureAwait(false);
            try {
                if (IsClosed || _stopTcs != null) {
                    Interlocked.Decrement(ref _inQueueInternal);
                    FailRejectedRequest(job, new InvalidOperationException($"WorkQueuer \"{Name}\" is stopping."));
                    SignalDrainIfComplete();
                    return false;
                }
                if (!channel.Writer.TryWrite(job)) {
                    Interlocked.Decrement(ref _inQueueInternal);
                    FailRejectedRequest(job, new InvalidOperationException($"Unable to queue work item on \"{Name}\"."));
                    SignalDrainIfComplete();
                    return false;
                }
                EnsureWorkerCapacityForDemand();
                return true;
            } finally {
                _lifecycleLock.Release();
            }
        }


        private int InitialWorkerCount() {
            var limit = EffectiveParallelLimit();
            return Math.Max(1, Math.Min(Environment.ProcessorCount, limit));
        }

        private void EnsureMinimumWorkers() {
            EnsureWorkers(InitialWorkerCount());
        }

        private void EnsureWorkerCapacityForDemand() {
            if (!IsRunning || _runCts == null || _runCts.IsCancellationRequested) return;

            var lastScale = new DateTime(Interlocked.Read(ref _lastWorkerScaleTicks), DateTimeKind.Utc);
            if (DateTime.UtcNow - lastScale < TimeSpan.FromMilliseconds(MinScaleIntervalMs)) {
                return;
            }

            var limit = EffectiveParallelLimit();
            var currentWorkers = Volatile.Read(ref _numberOfActualWorkers);
            var demand = Volatile.Read(ref _executingInternal) + Volatile.Read(ref _inQueueInternal);

            // Only scale if demand exceeds current capacity by 50% or more (integer math)
            var desiredWorkers = Math.Max(InitialWorkerCount(), Math.Min(limit, demand));
            if (desiredWorkers > currentWorkers && desiredWorkers * 2 > currentWorkers * 3) {
                Interlocked.Exchange(ref _lastWorkerScaleTicks, DateTime.UtcNow.Ticks);
                EnsureWorkers(desiredWorkers);
            }
        }

        private void EnsureWorkers(int desiredWorkers) {
            lock (_workersLock) {
                var runCts = _runCts;
                if (!IsRunning || runCts == null || runCts.IsCancellationRequested) return;

                var limit = EffectiveParallelLimit();
                desiredWorkers = Math.Min(limit, desiredWorkers);
                var token = runCts.Token;
                while (_numberOfActualWorkers < desiredWorkers) {
                    var workerId = Interlocked.Increment(ref _nextWorkerId);
                    Interlocked.Increment(ref _numberOfActualWorkers);
                    var workerTask = RunWorkerLoop(workerId, token);
                    _workerTasks.Add(workerTask);
                }
            }
        }

        private bool ShouldWorkersProcessQueuedItems() {
            return Active || _drainOnStop;
        }

        private async Task RunWorkerLoop(int workerId, CancellationToken ct) {
            try {
                while (ShouldWorkersProcessQueuedItems()) {
                    if (!ShouldWorkersProcessQueuedItems()) {
                        DrainQueuedJobs();
                        break;
                    }

                    // Read the channel WITHOUT creating it. The channel is created lazily on
                    // the first write (see WriteQueuedJob / WriteQueuedJobAsync) so that a
                    // ChannelCapacity assigned through an object initializer is honored even
                    // when the queuer is started from the constructor (init_started: true).
                    // Creating it here would read ChannelCapacity before the initializer has
                    // run, producing an unbounded channel and silently defeating the bound.
                    var channel = Volatile.Read(ref _workChannel);
                    if (channel == null) {
                        // No channel yet: nothing to read. Wait briefly for the first
                        // enqueue rather than busy-spinning.
                        await Task.Delay(2, _drainOnStop ? CancellationToken.None : ct).ConfigureAwait(false);
                        continue;
                    }

                    if (_drainOnStop) {
                        if (!channel.Reader.TryRead(out var drainingJob)) {
                            await Task.Delay(2).ConfigureAwait(false);
                            continue;
                        }
                        try {
                            await ProcessQueuedJob(drainingJob, workerId).ConfigureAwait(false);
                        } catch (Exception ex) {
                            LogWorkerException(workerId, ex);
                        }
                        continue;
                    }

                    try {
                        if (!await channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false)) {
                            break;
                        }
                    } catch (OperationCanceledException) when (_drainOnStop) {
                        continue;
                    }

                    if (!ShouldWorkersProcessQueuedItems()) {
                        DrainQueuedJobs();
                        break;
                    }

                    if (!channel.Reader.TryRead(out var job)) {
                        continue;
                    }

                    try {
                        await ProcessQueuedJob(job, workerId).ConfigureAwait(false);
                    } catch (Exception ex) {
                        LogWorkerException(workerId, ex);
                    }
                }
            } catch (OperationCanceledException) {
                if (_drainOnStop) {
                    await DrainWorkerAfterCancellation(workerId).ConfigureAwait(false);
                }
            } catch (ChannelClosedException) {
                // Dispose may complete the channel while a worker is waiting.
            } finally {
                Interlocked.Decrement(ref _numberOfActualWorkers);
            }
        }

        private async Task DrainWorkerAfterCancellation(int workerId) {
            while (_drainOnStop) {
                var channel = Volatile.Read(ref _workChannel);
                if (channel == null || !channel.Reader.TryRead(out var job)) {
                    await Task.Delay(2).ConfigureAwait(false);
                    continue;
                }
                try {
                    await ProcessQueuedJob(job, workerId).ConfigureAwait(false);
                } catch (Exception ex) {
                    LogWorkerException(workerId, ex);
                }
            }
        }

        private static void LogWorkerException(int workerId, Exception ex) {
            if (IsWorkQueuerLogEnabled()) {
                Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} failed to process a queued job: {ex}");
            }
        }

        private async Task ProcessQueuedJob(WorkJobExecutionRequest job, int workerId) {
            Interlocked.Decrement(ref _inQueueInternal);

            if (IsJobCancellationRequested(job)) {
                CancelOrphanedJob(job);
                return;
            }

            Interlocked.Exchange(ref _wentIdleTicks, DateTime.UtcNow.Ticks);

            ActiveJobs.TryAdd(job.id, job);
            Interlocked.Increment(ref _executingInternal);

            try {
                await ExecuteJob(job, workerId).ConfigureAwait(false);
            } catch (Exception ex) when (!IsFatalWorkerException(ex)) {
                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
                if (IsWorkQueuerLogEnabled()) {
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} terminated unexpectedly: {ex}");
                }
                if (!job.TaskCompletionSource.Task.IsCompleted) {
                    job._tcsNotifyDequeued.TrySetException(ex);
                    if (job.TaskCompletionSource.TrySetException(ex)) {
                        Interlocked.Increment(ref _workDoneInternal);
                        if (IsJobCancellationRequested(job)) {
                            Interlocked.Increment(ref _cancelledInternal);
                        }
                        job.Status = WorkJobRequestStatus.Failed;
                        DisposeJobCancellation(job);
                    }
                }
            } finally {
                ActiveJobs.TryRemove(job.id, out _);
                Interlocked.Decrement(ref _executingInternal);
                SignalDrainIfComplete();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsWorkQueuerLogEnabled() =>
            FiTechCoreExtensions.EnableStdoutLogs && FiTechCoreExtensions.EnabledSystemLogs.TryGetValue("FTH:WorkQueuer", out var enabled) && enabled;

        private void SignalDrainIfComplete() {
            var tcs = _drainTcs;
            var inQueue = Volatile.Read(ref _inQueueInternal);
            if (inQueue < 0) {
                if (IsWorkQueuerLogEnabled()) {
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"WARNING: _inQueueInternal went negative ({inQueue}), indicating a counting bug");
                }
            }
            if (tcs != null && IsDrainComplete()) {
                tcs.TrySetResult(true);
            }
        }

        private bool IsDrainComplete() {
            var channel = Volatile.Read(ref _workChannel);
            return Volatile.Read(ref _inQueueInternal) <= 0 && ActiveJobs.IsEmpty && (channel == null || channel.Reader.Count == 0);
        }

        private static bool IsJobCancellationRequested(WorkJobExecutionRequest job) {
            try {
                return job.Cancellation.IsCancellationRequested;
            } catch (ObjectDisposedException) {
                return true;
            }
        }

        private static bool IsFatalWorkerException(Exception exception) {
            return exception is OutOfMemoryException || exception is ThreadAbortException;
        }

        private static CancellationToken GetJobCancellationToken(WorkJobExecutionRequest job) {
            try {
                return job.Cancellation.Token;
            } catch (ObjectDisposedException) {
                return new CancellationToken(true);
            }
        }

        private static void DisposeJobCancellation(WorkJobExecutionRequest job) {
            try {
                job.Cancellation.Dispose();
            } catch (ObjectDisposedException) {
            }
        }

        private void CancelOrphanedJob(WorkJobExecutionRequest job) {
            Interlocked.Increment(ref _cancelledInternal);
            Interlocked.Increment(ref _workDoneInternal);
            CancellationToken cancellationToken = GetJobCancellationToken(job);
            job._tcsNotifyDequeued.TrySetCanceled(cancellationToken);
            job.TaskCompletionSource.TrySetCanceled(cancellationToken);
            job.Status = WorkJobRequestStatus.Failed;
            DisposeJobCancellation(job);
            SignalDrainIfComplete();
        }

        private void DrainQueuedJobs() {
            var channel = Volatile.Read(ref _workChannel);
            if (channel == null) return;
            while (channel.Reader.TryRead(out var orphan)) {
                Interlocked.Decrement(ref _inQueueInternal);
                CancelOrphanedJob(orphan);
            }
        }

        // Maximum absolute parallel limit to prevent resource exhaustion
        public static int AbsoluteMaxParallelLimit { get; set; } = 500;

        private int EffectiveParallelLimit() {
            var maxTasks = MaxParallelTasks;
            var absoluteLimit = AbsoluteMaxParallelLimit;
            if (_cachedMaxParallelTasksForLimit != maxTasks || _cachedAbsoluteMaxParallelLimitForLimit != absoluteLimit) {
                _cachedMaxParallelTasksForLimit = maxTasks;
                _cachedAbsoluteMaxParallelLimitForLimit = absoluteLimit;
                _cachedEffectiveParallelLimit = Math.Min(absoluteLimit, Math.Max(maxTasks, 1));
            }
            return _cachedEffectiveParallelLimit;
        }

        private int EffectiveChannelCapacity() {
            if (ChannelCapacity <= 0) {
                return Math.Max(1, EffectiveParallelLimit()) * 4;
            }
            return ChannelCapacity;
        }

        private Channel<WorkJobExecutionRequest> GetOrCreateChannel() {
            if (_workChannel != null) {
                return _workChannel;
            }
            lock (_channelLock) {
                if (_workChannel != null) {
                    return _workChannel;
                }
                if (ChannelCapacity > 0) {
                    var capacity = EffectiveChannelCapacity();
                    _workChannel = Channel.CreateBounded<WorkJobExecutionRequest>(new BoundedChannelOptions(capacity) {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleReader = false,
                        SingleWriter = false,
                        AllowSynchronousContinuations = false
                    });
                } else {
                    _workChannel = Channel.CreateUnbounded<WorkJobExecutionRequest>(new UnboundedChannelOptions {
                        SingleReader = false,
                        SingleWriter = false,
                        AllowSynchronousContinuations = false
                    });
                }
                return _workChannel;
            }
        }

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

            _lifecycleLock.Wait();
            try {
                if (IsClosed) {
                    FailRejectedRequest(request, new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed."));
                    return request;
                }
                if (_stopTcs != null) {
                    FailRejectedRequest(request, new InvalidOperationException($"WorkQueuer \"{Name}\" is stopping."));
                    return request;
                }

                if (Active) {
                    WriteQueuedJob(request, alreadyCounted: false);
                } else {
                    HeldJobs.Enqueue(request);
                }
                Interlocked.Increment(ref _totalWorkInternal);
            } finally {
                _lifecycleLock.Release();
            }

            _ = SafeInvoke(OnWorkEnqueued, request);
            return request;
        }

        public async Task<WorkJobExecutionRequest> EnqueueAsync(WorkJob job, CancellationToken? requestCancellation = null) {
            var request = new WorkJobExecutionRequest(job, requestCancellation) {
                EnqueuedTime = DateTime.UtcNow,
                Status = WorkJobRequestStatus.Queued,
                WorkQueuer = this
            };
            if (FiTechCoreExtensions.DebugTasks) {
                request.StackTrace = new StackTrace();
            }

            bool writeToChannel;
            await _lifecycleLock.WaitAsync().ConfigureAwait(false);
            try {
                if (IsClosed) {
                    FailRejectedRequest(request, new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed."));
                    return request;
                }
                if (_stopTcs != null) {
                    FailRejectedRequest(request, new InvalidOperationException($"WorkQueuer \"{Name}\" is stopping."));
                    return request;
                }

                writeToChannel = Active;
                if (!writeToChannel) {
                    HeldJobs.Enqueue(request);
                }
            } finally {
                _lifecycleLock.Release();
            }

            if (writeToChannel) {
                if (!await WriteQueuedJobAsync(request).ConfigureAwait(false)) {
                    return request;
                }
            }

            Interlocked.Increment(ref _totalWorkInternal);

            _ = SafeInvoke(OnWorkEnqueued, request);
            return request;
        }

        private static void FailRejectedRequest(WorkJobExecutionRequest request, Exception exception) {
            request.Status = WorkJobRequestStatus.Failed;
            request._tcsNotifyDequeued.TrySetException(exception);
            request.TaskCompletionSource.TrySetException(exception);
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
        public Task<WorkJobExecutionRequest> EnqueueTaskAsync(Func<CancellationToken, ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            return EnqueueAsync(retv);
        }


        private async Task ExecuteJob(WorkJobExecutionRequest job, int workerId) {
            var now = DateTime.UtcNow;
            job.TimeInQueue = now - (job.EnqueuedTime ?? now);
            await SafeInvoke(OnWorkDequeued, job).ConfigureAwait(false);

            var startTimestamp = Stopwatch.GetTimestamp();
            Exception terminalException = null;

            try {
                // Telemetry
                if (job.WorkJob.AllowTelemetry) {
                    job.LoggingActivity = Fi.Tech.CreateTelemetryActivity(job.WorkJob?.Name ?? "Unnamed Task", ActivityKind.Internal);
                    if (job.LoggingActivity != null) {
                        if (_defaultLoggingTags != null) {
                            foreach (var kv in _defaultLoggingTags) job.LoggingActivity.AddTag(kv.Key, kv.Value);
                        }
                        if (job.WorkJob._additionalTelemetryTags != null) {
                            foreach (var kv in job.WorkJob._additionalTelemetryTags) job.LoggingActivity.AddTag(kv.Key, kv.Value);
                        }
                        job.LoggingActivity.AddTag("WorkQueuer", Name);
                        job.LoggingActivity.Start();
                        job.LoggingActivity.SetStartTime(now);
                    }
                }

                job.DequeuedTime = now;
                job._tcsNotifyDequeued.TrySetResult(0);
                job.Status = WorkJobRequestStatus.Running;

                if (job.WorkJob.action != null) {
                    CancellationTokenSource ctsLinked = null;
                    CancellationToken actionToken;
                    if (job.RequestCancellation.CanBeCanceled) {
                        ctsLinked = CancellationTokenSource.CreateLinkedTokenSource(GetJobCancellationToken(job), job.RequestCancellation);
                        actionToken = ctsLinked.Token;
                    } else {
                        actionToken = GetJobCancellationToken(job);
                    }

                    try {
                        var actionTask = job.WorkJob.action(actionToken);
                        await actionTask.ConfigureAwait(false);
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

                if (IsWorkQueuerLogEnabled()) {
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} executed OK");
                }
            } catch (Exception execEx) {
                job.Status = WorkJobRequestStatus.Failed;
                if (job.WorkJob.AllowTelemetry && job.LoggingActivity != null) {
                    job.LoggingActivity.AddTag("Exception", JsonConvert.SerializeObject(execEx.ToExceptionArray()));
                    job.LoggingActivity.SetStatus(ActivityStatusCode.Error);
                }

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

                if (IsWorkQueuerLogEnabled()) {
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} thrown an Exception: {execEx.Message}");
                }
            } finally {
                try {
                    var endTimestamp = Stopwatch.GetTimestamp();
                    var elapsedTicks = endTimestamp - startTimestamp;
                    var elapsed = TimeSpan.FromTicks((long)(elapsedTicks * StopwatchTickToTimeSpanTicks));
                    var completedTime = now + elapsed;
                    job.CompletedTime = completedTime;
                    job.TimeToComplete = elapsed;

                    // Only set Finished if we didn't fail earlier
                    if (job.Status != WorkJobRequestStatus.Failed) {
                        job.Status = WorkJobRequestStatus.Finished;
                    }

                    await SafeInvoke(OnWorkComplete, job).ConfigureAwait(false);

                    Interlocked.Increment(ref _workDoneInternal);
                    if (IsJobCancellationRequested(job)) {
                        Interlocked.Increment(ref _cancelledInternal);
                    }
                    DisposeJobCancellation(job);

                    job.LoggingActivity?.SetEndTime(completedTime);
                    job.LoggingActivity?.Dispose();

                    if (terminalException != null) {
                        job.TaskCompletionSource.TrySetException(terminalException);
                        _ = job.TaskCompletionSource.Task.Exception; // observe
                    } else {
                        job.TaskCompletionSource.TrySetResult(0);
                    }

                    Interlocked.Add(ref _totalTaskResolutionTimeTicks, elapsed.Ticks);
                    Interlocked.Exchange(ref _wentIdleTicks, completedTime.Ticks);
                } catch (Exception cleanupEx) {
                    if (Debugger.IsAttached) Debugger.Break();
                    if (IsWorkQueuerLogEnabled()) {
                        Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker cleanup error: {cleanupEx.Message}");
                    }
                }
                if (IsWorkQueuerLogEnabled()) {
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {workerId} cleanup OK");
                }
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
                _scheduledTaskQueue.Add(entry);
                RescheduleConsolidatedTimerUnsafe();
            }
        }

        private void RescheduleConsolidatedTimerUnsafe() {
            // Must be called within _scheduledTasksLock
            if (_scheduledTaskQueue.Count == 0) {
                _consolidatedTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                return;
            }

            var nextEntry = _scheduledTaskQueue.Min;
            var now = DateTime.UtcNow;
            var delay = nextEntry.ScheduledTime - now;
            var delayMs = Math.Max(0, (long)delay.TotalMilliseconds);

            // Cap timer at 1 minute to handle clock changes and long waits
            const int maxTimerIntervalMs = 60000;
            var timerDelay = (int)Math.Min(delayMs, maxTimerIntervalMs);

            if (_consolidatedTimer == null) {
                _consolidatedTimer = new Timer(
                    _ => OnConsolidatedTimerFired(),
                    null,
                    timerDelay,
                    Timeout.Infinite
                );
            } else {
                _consolidatedTimer.Change(timerDelay, Timeout.Infinite);
            }
        }

        private void OnConsolidatedTimerFired() {
            if (_isClosed || !_isRunning) return;
            List<ScheduledTaskEntry> entriesToExecute = null;
            List<ScheduledTaskEntry> entriesToReschedule = null;
            List<ScheduledTaskEntry> entriesToCleanup = null;

            lock (_scheduledTasksLock) {
                var now = DateTime.UtcNow;
                const int timerPrecisionMs = 50;

                // Process all entries that are due
                while (_scheduledTaskQueue.Count > 0) {
                    var entry = _scheduledTaskQueue.Min;
                    var timeUntilScheduled = entry.ScheduledTime - now;

                    // If not yet time (within precision window), stop processing
                    if (timeUntilScheduled.TotalMilliseconds > timerPrecisionMs) {
                        break;
                    }

                    // Remove from queue
                    _scheduledTaskQueue.Remove(entry);

                    if (entry.Cancellation.IsCancellationRequested) {
                        entriesToCleanup ??= new List<ScheduledTaskEntry>();
                        entriesToCleanup.Add(entry);
                        continue;
                    }

                    // Check if WorkQueuer is active
                    if (!IsRunning || !Active) {
                        if (entry.Options.FireIfMissed) {
                            _missedSchedules.Add(entry);
                        }
                        // For recurring tasks, calculate next occurrence
                        if (entry.Options.RecurrenceInterval.HasValue) {
                            entriesToReschedule ??= new List<ScheduledTaskEntry>();
                            entriesToReschedule.Add(entry);
                        } else {
                            entriesToCleanup ??= new List<ScheduledTaskEntry>();
                            entriesToCleanup.Add(entry);
                        }
                        continue;
                    }

                    entriesToExecute ??= new List<ScheduledTaskEntry>();
                    entriesToExecute.Add(entry);
                }

                // Reschedule timer for next batch
                RescheduleConsolidatedTimerUnsafe();
            }

            // Execute outside the lock
            if (entriesToExecute != null) {
                foreach (var entry in entriesToExecute) {
                    ExecuteScheduledJob(entry);
                }
            }

            // Cleanup outside the lock
            if (entriesToCleanup != null) {
                foreach (var entry in entriesToCleanup) {
                    CleanupSchedule(entry);
                }
            }

            // Reschedule recurring tasks outside the lock
            if (entriesToReschedule != null) {
                foreach (var entry in entriesToReschedule) {
                    RescheduleEntry(entry);
                }
            }
        }

        private void ExecuteScheduledJob(ScheduledTaskEntry entry) {
            lock (_scheduledTasksLock) {
                if (entry.IsExecuting) {
                    // Already running, skip this occurrence
                    if (entry.Options.RecurrenceInterval.HasValue) {
                        // Re-add to queue for rescheduling
                        _scheduledTaskQueue.Add(entry);
                        RescheduleConsolidatedTimerUnsafe();
                    }
                    return;
                }
                entry.IsExecuting = true;
            }

            var request = Enqueue(entry.Job, entry.Cancellation.Token);

            request.GetAwaiter().OnCompleted(() => {
                lock (_scheduledTasksLock) {
                    entry.IsExecuting = false;

                    if (entry.Options.RecurrenceInterval.HasValue && !entry.Cancellation.IsCancellationRequested) {
                        // Recalculate next scheduled time
                        var now = DateTime.UtcNow;
                        var nextRun = entry.ScheduledTime;
                        var interval = entry.Options.RecurrenceInterval.Value;
                        do {
                            nextRun = nextRun.Add(interval);
                        } while (nextRun <= now);
                        entry.ScheduledTime = nextRun;
                        
                        _scheduledTaskQueue.Add(entry);
                        RescheduleConsolidatedTimerUnsafe();
                    } else {
                        _scheduledTasks.Remove(entry.Identifier);
                        entry.Cancellation?.Dispose();
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
            
            lock (_scheduledTasksLock) {
                _scheduledTaskQueue.Add(entry);
                RescheduleConsolidatedTimerUnsafe();
            }
        }

        private void CleanupSchedule(ScheduledTaskEntry entry) {
            lock (_scheduledTasksLock) {
                _scheduledTaskQueue.Remove(entry);
                entry.Cancellation?.Dispose();
                _scheduledTasks.Remove(entry.Identifier);
            }
        }

        private void UnscheduleInternal(string identifier) {
            if (_scheduledTasks.TryGetValue(identifier, out var entry)) {
                entry.Cancellation?.Cancel();
                lock (_scheduledTasksLock) {
                    _scheduledTaskQueue.Remove(entry);
                    _scheduledTasks.Remove(identifier);
                }
                entry.Cancellation?.Dispose();
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
            RunMissedSchedules(TakeMissedSchedules());
        }

        private List<ScheduledTaskEntry> TakeMissedSchedules() {
            lock (_scheduledTasksLock) {
                var missed = new List<ScheduledTaskEntry>(_missedSchedules);
                _missedSchedules.Clear();
                return missed;
            }
        }

        private void RunMissedSchedules(List<ScheduledTaskEntry> missed) {
            foreach (var entry in missed) {
                try {
                    if (entry.Options.FireIfMissed && !entry.Cancellation.IsCancellationRequested) {
                        ExecuteScheduledJob(entry);
                    }
                } catch (ObjectDisposedException) {
                    // CTS was disposed between being added to _missedSchedules
                    // and ProcessMissedSchedules running; safe to skip.
                }
            }
        }

        #endregion

        private void DisposeScheduledTasks() {
            lock (_scheduledTasksLock) {
                _consolidatedTimer?.Dispose();
                _consolidatedTimer = null;
                _scheduledTaskQueue.Clear();
                foreach (var entry in _scheduledTasks.Values) {
                    entry.Cancellation?.Cancel();
                    entry.Cancellation?.Dispose();
                }
                _scheduledTasks.Clear();
                _missedSchedules.Clear();
            }
        }

        private bool TakeDisposeOwnership() => Interlocked.Exchange(ref _disposed, 1) == 0;

        public void Dispose() {
            if (!TakeDisposeOwnership()) return;
            IsClosed = true;
            // Use a synchronous wait with timeout to avoid deadlocks
            // when Dispose() is called from a sync context
            try {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30))) {
                    Stop(true).Wait(cts.Token);
                }
            } catch (OperationCanceledException) {
                // Timeout - force shutdown: cancel remaining work
                try { _runCts?.Cancel(); } catch { }
            } catch (AggregateException aex) when (aex.InnerExceptions.All(e => e is OperationCanceledException)) {
                // Stop() was cancelled via Wait(cts.Token) wrapped in AggregateException
                try { _runCts?.Cancel(); } catch { }
            } catch (Exception) {
                // Ignore other exceptions during dispose
            }

            try { FailHeldJobs(new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed.")); } catch { }
            try { DisposeScheduledTasks(); } catch { }
            try { GetOrCreateChannel().Writer.TryComplete(); } catch { }
            try { _runCts?.Dispose(); } catch { }
        }

        public async ValueTask DisposeAsync() {
            if (!TakeDisposeOwnership()) return;
            IsClosed = true;
            try {
                await Stop(true).ConfigureAwait(false);
            } catch (Exception) {
                // Ignore exceptions during async dispose
            }

            try { FailHeldJobs(new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed.")); } catch { }
            try { DisposeScheduledTasks(); } catch { }
            try { GetOrCreateChannel().Writer.TryComplete(); } catch { }
            try { _runCts?.Dispose(); } catch { }
        }
    }
}
