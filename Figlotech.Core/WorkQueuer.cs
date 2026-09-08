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
        [JsonIgnore]
        [System.Text.Json.Serialization.JsonIgnore]
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
        private readonly int __qid = Interlocked.Increment(ref qid_increment);
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

        public static int DefaultSleepInterval = 25;

        // Volatile-backed state flags for cross-thread visibility
        private volatile bool _isClosed;
        private volatile bool _isRunning;
        private volatile bool _active;

        public bool IsClosed { get => _isClosed; private set => _isClosed = value; }
        public bool IsRunning { get => _isRunning; private set => _isRunning = value; }
        public bool Active { get => _active; private set => _active = value; }

        private Channel<WorkJobExecutionRequest> _workChannel;
        private readonly TaskCompletionSource<Channel<WorkJobExecutionRequest>> _channelReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly object _channelLock = new object();
        /// <summary>
        /// Buffer capacity, captured on the first enqueue. Zero means unbounded.
        /// Use a positive capacity and await EnqueueAsync to apply producer backpressure.
        /// Work held before Start and concurrent producers awaiting admission also consume memory.
        /// </summary>
        public int ChannelCapacity { get; set; } = 0;
        private CancellationTokenSource _runCts;
        private CancellationTokenSource _workerCts;
        private readonly object _workersLock = new object();
        private readonly List<Task> _workerTasks = new List<Task>();
        // Intentionally not disposed: public Stop/Enqueue calls can still be waiting on it; disposal could fault them, and GC will reclaim it.
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213", Justification = "Concurrent public calls may still use the semaphore after disposal; no wait handle is allocated.")]
        private readonly SemaphoreSlim _lifecycleLock = new SemaphoreSlim(1, 1);
        private volatile bool _drainOnStop;
        private int _numberOfActualWorkers;
        private int _nextWorkerId;
        private int _disposed;

        // Signaled when all queued and active work has drained (used by Stop)
        private readonly object _drainLock = new object();
        private TaskCompletionSource<bool> _drainTcs;
        private int _outstandingWork;
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
            public CancellationToken Token { get; set; }
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
                if (IsClosed) throw new ObjectDisposedException(nameof(WorkQueuer));
                if (IsRunning || (_stopTcs != null && !_stopTcs.Task.IsCompleted)) return;

                lock (_scheduledTasksLock) {
                    missedSchedules = TakeMissedSchedules();
                    Active = true;
                }
                IsRunning = true;
                _stopTcs = null;
                _drainOnStop = false;
                _runCts?.Dispose();
                _runCts = new CancellationTokenSource();
                _workerCts?.Dispose();
                _workerCts = new CancellationTokenSource();

                EnsureMinimumWorkers();

                FlushHeldJobsToQueue();
                EnsureWorkerCapacityForDemand();
                lock (_scheduledTasksLock) {
                    RescheduleConsolidatedTimerUnsafe();
                }
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
                _workerCts?.Cancel();
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
            // Includes accepted held work, pending bounded writes, and jobs being dequeued.
            // Workers themselves remain suspended on the channel until Stop/Dispose.
            return WaitForDrainAsync();
        }

        private Task WaitForDrainAsync() {
            lock (_drainLock) {
                if (_outstandingWork == 0) return Task.CompletedTask;
                _drainTcs ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                return _drainTcs.Task;
            }
        }

        private void ReserveWork() {
            lock (_drainLock) {
                ++_outstandingWork;
            }
            Interlocked.Increment(ref _totalWorkInternal);
        }

        private void ResolveWork() {
            lock (_drainLock) {
                if (--_outstandingWork == 0) {
                    _drainTcs?.TrySetResult(true);
                    _drainTcs = null;
                }
            }
        }

        private void FlushHeldJobsToQueue() {
            // Start holds _lifecycleLock throughout this flush, so Stop cannot drain the channel between
            // this write and its final drain. A held job is therefore either queued before Stop begins or
            // failed by Stop while still held.
            while (HeldJobs.TryDequeue(out var job)) {
                try {
                    WriteQueuedJob(job, alreadyCounted: false);
                } catch (Exception ex) {
                    FailAcceptedRequest(job, ex);
                }
            }
        }

        private void FailHeldJobs(Exception exception) {
            while (HeldJobs.TryDequeue(out var job)) {
                FailAcceptedRequest(job, exception);
            }
        }

        private void FailAcceptedRequest(WorkJobExecutionRequest job, Exception exception) {
            FailRejectedRequest(job, exception);
            Interlocked.Increment(ref _workDoneInternal);
            Interlocked.Increment(ref _cancelledInternal);
            ResolveWork();
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

        private async Task WriteQueuedJobAsync(WorkJobExecutionRequest job, Channel<WorkJobExecutionRequest> channel, CancellationToken runToken) {
            using var linked = job.RequestCancellation.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(runToken, job.RequestCancellation)
                : null;
            var cancellationToken = linked?.Token ?? runToken;
            try {
                // The reservation is included in drain accounting before leaving the lifecycle
                // lock. Stop cancels pending writes and waits for their reservations to resolve,
                // so Channels can own admission without a readiness/retry race or producer herd.
                await channel.Writer.WriteAsync(job, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) {
                Interlocked.Decrement(ref _inQueueInternal);
                FailAcceptedRequest(job, ex);
                throw;
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
            if (!Active || _runCts == null || _runCts.IsCancellationRequested) return;
            var demand = Volatile.Read(ref _outstandingWork);
            var desiredWorkers = Math.Max(InitialWorkerCount(), Math.Min(EffectiveParallelLimit(), demand));
            if (desiredWorkers > NumberOfActualWorkers) {
                EnsureWorkers(desiredWorkers);
            }
        }

        private void EnsureWorkers(int desiredWorkers) {
            lock (_workersLock) {
                var runCts = _runCts;
                if (!IsRunning || runCts == null || runCts.IsCancellationRequested) return;

                var limit = EffectiveParallelLimit();
                desiredWorkers = Math.Min(limit, desiredWorkers);
                var token = _workerCts.Token;
                while (_numberOfActualWorkers < desiredWorkers) {
                    var workerId = Interlocked.Increment(ref _nextWorkerId);
                    Interlocked.Increment(ref _numberOfActualWorkers);
                    var workerTask = Task.Run(() => RunWorkerLoop(workerId, token));
                    _workerTasks.Add(workerTask);
                }
            }
        }

        private bool ShouldWorkersProcessQueuedItems() {
            return Active || _drainOnStop;
        }

        private async Task RunWorkerLoop(int workerId, CancellationToken ct) {
            try {
                // Preserve lazy channel creation so object-initializer capacity is honored.
                // Idle workers are suspended tasks, not sleeping or spinning threads.
                var channel = await _channelReady.Task.WaitAsync(ct).ConfigureAwait(false);
                while (ShouldWorkersProcessQueuedItems()) {
                    // ReadAsync pairs an item with one waiter; WaitToReadAsync would wake all
                    // readers to compete for the same item when the queue is mostly idle.
                    var job = await channel.Reader.ReadAsync(ct).ConfigureAwait(false);
                    using (job) {
                        try {
                            if (!ShouldWorkersProcessQueuedItems()) {
                                Interlocked.Decrement(ref _inQueueInternal);
                                CancelOrphanedJob(job);
                            } else {
                                await ProcessQueuedJob(job, workerId).ConfigureAwait(false);
                            }
                        } catch (Exception ex) {
                            LogWorkerException(workerId, ex);
                        }
                    }
                }
            } catch (OperationCanceledException) when (ct.IsCancellationRequested) {
                // Stop cancels readers only after the accepted work has resolved.
            } catch (ChannelClosedException) {
                // Disposal can complete the channel.
            } finally {
                Interlocked.Decrement(ref _numberOfActualWorkers);
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
                ResolveWork();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsWorkQueuerLogEnabled() =>
            FiTechCoreExtensions.EnableStdoutLogs && FiTechCoreExtensions.EnabledSystemLogs.TryGetValue("FTH:WorkQueuer", out var enabled) && enabled;


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
            ResolveWork();
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
            return Math.Max(1, Math.Min(AbsoluteMaxParallelLimit, Math.Max(MaxParallelTasks, 1)));
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
                _channelReady.TrySetResult(_workChannel);
                return _workChannel;
            }
        }

        public async Task AccompanyJob(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var wj = EnqueueTask(a, exceptionHandler, finished);
            await wj;
        }

        private WorkJobExecutionRequest CreateRequest(WorkJob job, CancellationToken? requestCancellation) {
            if (job == null) throw new ArgumentNullException(nameof(job));
            return new WorkJobExecutionRequest(job, requestCancellation) {
                EnqueuedTime = DateTime.UtcNow,
                WorkQueuer = this,
                StackTrace = FiTechCoreExtensions.DebugTasks ? new StackTrace() : null
            };
        }

        private bool CanAcceptRequest(WorkJobExecutionRequest request) {
            if (IsClosed) {
                FailRejectedRequest(request, new ObjectDisposedException(nameof(WorkQueuer)));
                return false;
            }
            if (_stopTcs != null) {
                FailRejectedRequest(request, new InvalidOperationException($"WorkQueuer \"{Name}\" is stopping."));
                return false;
            }
            return true;
        }

        public WorkJobExecutionRequest Enqueue(WorkJob job, CancellationToken? requestCancellation = null) {
            var request = CreateRequest(job, requestCancellation);
            _lifecycleLock.Wait();
            try {
                if (!CanAcceptRequest(request)) return request;
                ReserveWork();
                try {
                    if (Active) {
                        WriteQueuedJob(request, alreadyCounted: false);
                    } else {
                        HeldJobs.Enqueue(request);
                    }
                } catch (Exception ex) {
                    Interlocked.Decrement(ref _totalWorkInternal);
                    FailRejectedRequest(request, ex);
                    ResolveWork();
                    throw;
                }
            } finally {
                _lifecycleLock.Release();
            }
            _ = SafeInvoke(OnWorkEnqueued, request);
            return request;
        }

        public async Task<WorkJobExecutionRequest> EnqueueAsync(WorkJob job, CancellationToken? requestCancellation = null) {
            var request = CreateRequest(job, requestCancellation);
            Channel<WorkJobExecutionRequest> channel = null;
            CancellationToken runToken = default;
            await _lifecycleLock.WaitAsync().ConfigureAwait(false);
            try {
                if (!CanAcceptRequest(request)) return request;
                ReserveWork();
                if (Active) {
                    channel = GetOrCreateChannel();
                    runToken = _runCts.Token;
                    Interlocked.Increment(ref _inQueueInternal);
                    EnsureWorkerCapacityForDemand();
                } else {
                    HeldJobs.Enqueue(request);
                }
            } finally {
                _lifecycleLock.Release();
            }
            if (channel != null) {
                await WriteQueuedJobAsync(request, channel, runToken).ConfigureAwait(false);
            }
            _ = SafeInvoke(OnWorkEnqueued, request);
            return request;
        }

        private static void FailRejectedRequest(WorkJobExecutionRequest request, Exception exception) {
            request.Status = WorkJobRequestStatus.Failed;
            request._tcsNotifyDequeued.TrySetException(exception);
            request.TaskCompletionSource.TrySetException(exception);
            request.Dispose();
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
                            await InvokeExceptionHandlers(job, execEx, ex).ConfigureAwait(false);
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

                // Completion callbacks run for both handled and unhandled failures.
                if (job.WorkJob.finished != null) {
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
            foreach (Func<WorkJobExecutionRequest, Task> handler in ev.GetInvocationList()) {
                try {
                    await handler(r).ConfigureAwait(false);
                } catch (Exception ex) {
                    Fi.Tech.SwallowException(ex);
                }
            }
        }

        private async Task InvokeExceptionHandlers(WorkJobExecutionRequest request, Exception executionException, Exception handlerException) {
            var handlers = OnExceptionInHandler;
            if (handlers == null) return;
            List<Exception> exceptions = null;
            foreach (Func<WorkJobExecutionRequest, Exception, Exception, Task> handler in handlers.GetInvocationList()) {
                try {
                    await handler(request, executionException, handlerException).ConfigureAwait(false);
                } catch (Exception ex) {
                    (exceptions ??= new List<Exception>()).Add(ex);
                }
            }
            if (exceptions != null) throw new AggregateException(exceptions);
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
            if (options.RecurrenceInterval.HasValue && options.RecurrenceInterval.Value <= TimeSpan.Zero) {
                throw new ArgumentOutOfRangeException(nameof(options), "RecurrenceInterval must be positive.");
            }
            // Snapshot options so a caller cannot invalidate the interval after validation.
            var snapshot = new ScheduledTaskOptions {
                ScheduledTime = options.ScheduledTime,
                RecurrenceInterval = options.RecurrenceInterval,
                FireIfMissed = options.FireIfMissed,
                CancellationToken = options.CancellationToken
            };
            ScheduledTaskEntry previous;
            lock (_scheduledTasksLock) {
                if (IsClosed) throw new ObjectDisposedException(nameof(WorkQueuer));
                previous = RemoveScheduleUnsafe(identifier);
                var cts = CancellationTokenSource.CreateLinkedTokenSource(snapshot.CancellationToken ?? CancellationToken.None);
                var entry = new ScheduledTaskEntry {
                    Identifier = identifier,
                    Created = DateTime.UtcNow,
                    Job = job,
                    Options = snapshot,
                    Cancellation = cts,
                    Token = cts.Token,
                    ScheduledTime = snapshot.ScheduledTime ?? DateTime.UtcNow
                };
                _scheduledTasks[identifier] = entry;
                _scheduledTaskQueue.Add(entry);
                RescheduleConsolidatedTimerUnsafe();
            }
            CancelSchedule(previous);
        }

        private void RescheduleConsolidatedTimerUnsafe() {
            // Stopped schedules are reconciled by Start; they need no periodic wakeups.
            if (IsClosed || !Active || _scheduledTaskQueue.Count == 0) {
                _consolidatedTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                return;
            }
            var delay = _scheduledTaskQueue.Min.ScheduledTime - DateTime.UtcNow;
            var delayMs = Math.Max(0, (long)Math.Ceiling(delay.TotalMilliseconds));
            var timerDelay = (int)Math.Min(delayMs, 60000);
            if (_consolidatedTimer == null) {
                _consolidatedTimer = new Timer(_ => OnConsolidatedTimerFired(), null, timerDelay, Timeout.Infinite);
            } else {
                _consolidatedTimer.Change(timerDelay, Timeout.Infinite);
            }
        }

        private void OnConsolidatedTimerFired() {
            List<ScheduledTaskEntry> entries = null;
            lock (_scheduledTasksLock) {
                if (IsClosed) return;
                var now = DateTime.UtcNow;
                while (_scheduledTaskQueue.Count > 0 && _scheduledTaskQueue.Min.ScheduledTime <= now) {
                    var entry = _scheduledTaskQueue.Min;
                    _scheduledTaskQueue.Remove(entry);
                    if (entry.Token.IsCancellationRequested) {
                        CleanupScheduleUnsafe(entry);
                    } else if (!Active) {
                        HandleMissedScheduleUnsafe(entry, now);
                    } else {
                        (entries ??= new List<ScheduledTaskEntry>()).Add(entry);
                    }
                }
                RescheduleConsolidatedTimerUnsafe();
            }
            if (entries != null) {
                foreach (var entry in entries) ExecuteScheduledJob(entry);
            }
        }

        private bool IsCurrentScheduleUnsafe(ScheduledTaskEntry entry) {
            return _scheduledTasks.TryGetValue(entry.Identifier, out var current) && ReferenceEquals(current, entry);
        }

        private void ExecuteScheduledJob(ScheduledTaskEntry entry) {
            lock (_scheduledTasksLock) {
                if (!IsCurrentScheduleUnsafe(entry) || entry.IsExecuting) return;
                if (entry.Token.IsCancellationRequested || IsClosed) {
                    CleanupScheduleUnsafe(entry);
                    return;
                }
                if (!Active) {
                    HandleMissedScheduleUnsafe(entry, DateTime.UtcNow);
                    return;
                }
                _scheduledTaskQueue.Remove(entry);
                entry.IsExecuting = true;
            }
            _ = ExecuteScheduledJobAsync(entry);
        }

        private async Task ExecuteScheduledJobAsync(ScheduledTaskEntry entry) {
            try {
                // Bounded queues suspend this admission asynchronously. At most one execution
                // (including admission) is in flight per schedule, even for recurring jobs.
                var request = await EnqueueAsync(entry.Job, entry.Token).ConfigureAwait(false);
                await request.TaskCompletionSource.Task.ConfigureAwait(false);
            } catch (Exception ex) {
                // Execution already reports job errors; admission can also be cancelled by Stop.
                LogWorkerException(0, ex);
            } finally {
                lock (_scheduledTasksLock) {
                    entry.IsExecuting = false;
                    if (!IsCurrentScheduleUnsafe(entry)) {
                        entry.Cancellation.Dispose();
                    } else if (IsClosed || entry.Token.IsCancellationRequested) {
                        CleanupScheduleUnsafe(entry);
                    } else if (entry.Options.RecurrenceInterval.HasValue && AdvanceScheduleUnsafe(entry, DateTime.UtcNow)) {
                        _scheduledTaskQueue.Add(entry);
                    } else {
                        CleanupScheduleUnsafe(entry);
                    }
                    RescheduleConsolidatedTimerUnsafe();
                }
            }
        }

        private static bool AdvanceScheduleUnsafe(ScheduledTaskEntry entry, DateTime now) {
            var intervalTicks = entry.Options.RecurrenceInterval.Value.Ticks;
            var elapsedTicks = Math.Max(0, now.Ticks - entry.ScheduledTime.Ticks);
            var intervals = elapsedTicks / intervalTicks + 1;
            // No representable next occurrence: retire the schedule instead of overflowing
            // or looping through every missed occurrence on a thread-pool thread.
            if (intervals > (DateTime.MaxValue.Ticks - entry.ScheduledTime.Ticks) / intervalTicks) return false;
            entry.ScheduledTime = entry.ScheduledTime.AddTicks(intervals * intervalTicks);
            return true;
        }

        private void HandleMissedScheduleUnsafe(ScheduledTaskEntry entry, DateTime now) {
            if (entry.Options.FireIfMissed) {
                if (!_missedSchedules.Contains(entry)) _missedSchedules.Add(entry);
            } else if (entry.Options.RecurrenceInterval.HasValue && AdvanceScheduleUnsafe(entry, now)) {
                _scheduledTaskQueue.Add(entry);
            } else {
                CleanupScheduleUnsafe(entry);
            }
        }

        private void CleanupScheduleUnsafe(ScheduledTaskEntry entry) {
            if (!IsCurrentScheduleUnsafe(entry)) return;
            _scheduledTaskQueue.Remove(entry);
            _missedSchedules.Remove(entry);
            _scheduledTasks.Remove(entry.Identifier);
            if (!entry.IsExecuting) entry.Cancellation.Dispose();
        }

        private ScheduledTaskEntry RemoveScheduleUnsafe(string identifier) {
            if (!_scheduledTasks.TryGetValue(identifier, out var entry)) return null;
            _scheduledTaskQueue.Remove(entry);
            _missedSchedules.Remove(entry);
            _scheduledTasks.Remove(identifier);
            return entry;
        }

        private void CancelSchedule(ScheduledTaskEntry entry) {
            if (entry == null) return;
            // Cancellation can run user callbacks: never invoke it under the scheduler lock.
            try {
                entry.Cancellation.Cancel();
            } catch (ObjectDisposedException) {
                // An execution that was just removed can finish before cancellation.
            } catch (AggregateException ex) {
                Fi.Tech.SwallowException(ex);
            } finally {
                lock (_scheduledTasksLock) {
                    if (!entry.IsExecuting) entry.Cancellation.Dispose();
                }
            }
        }

        public void Unschedule(string identifier) {
            ScheduledTaskEntry entry;
            lock (_scheduledTasksLock) {
                entry = RemoveScheduleUnsafe(identifier);
                RescheduleConsolidatedTimerUnsafe();
            }
            CancelSchedule(entry);
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

        private List<ScheduledTaskEntry> TakeMissedSchedules() {
            lock (_scheduledTasksLock) {
                var now = DateTime.UtcNow;
                while (_scheduledTaskQueue.Count > 0 && _scheduledTaskQueue.Min.ScheduledTime <= now) {
                    var entry = _scheduledTaskQueue.Min;
                    _scheduledTaskQueue.Remove(entry);
                    if (entry.Token.IsCancellationRequested) {
                        CleanupScheduleUnsafe(entry);
                    } else {
                        HandleMissedScheduleUnsafe(entry, now);
                    }
                }
                var missed = new List<ScheduledTaskEntry>(_missedSchedules);
                _missedSchedules.Clear();
                return missed;
            }
        }

        private void RunMissedSchedules(List<ScheduledTaskEntry> missed) {
            foreach (var entry in missed) ExecuteScheduledJob(entry);
        }

        #endregion

        private void DisposeScheduledTasks() {
            ScheduledTaskEntry[] entries;
            lock (_scheduledTasksLock) {
                _consolidatedTimer?.Dispose();
                _consolidatedTimer = null;
                entries = _scheduledTasks.Values.ToArray();
                _scheduledTaskQueue.Clear();
                _scheduledTasks.Clear();
                _missedSchedules.Clear();
            }
            foreach (var entry in entries) CancelSchedule(entry);
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
            try { _workerCts?.Dispose(); } catch { }
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
            try { _workerCts?.Dispose(); } catch { }
        }
    }
}
