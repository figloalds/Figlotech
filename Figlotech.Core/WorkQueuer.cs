using System;
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
        public string[] EnqueuingContextStackTrace { get; private set; }
        public WorkJobExecutionStat WorkJobDetails { get; private set; }
        public WorkJobException(string message, WorkJobExecutionRequest job, Exception inner) : base(message, inner) {
            this.EnqueuingContextStackTrace = job.StackTraceText.Split('\n').Select(x => x?.Trim()).ToArray();
        }
    }

    public sealed class WorkJobExecutionRequest : IDisposable {
        public int id = ++idGen;
        private static int idGen = 0;

        public WorkJob WorkJob { get; internal set; }
        public CancellationTokenSource Cancellation { get; internal set; }
        public CancellationToken RequestCancellation { get; internal set; }

        private bool _disposed;
        public WorkJobExecutionRequest(WorkJob job, CancellationToken? requestCancellation = null) {
            WorkJob = job;
            Cancellation = new CancellationTokenSource();
            RequestCancellation = requestCancellation ?? CancellationToken.None;
            Status = WorkJobRequestStatus.Queued;
        }

        public WorkJobRequestStatus Status {  get; internal set; }
        public DateTime? EnqueuedTime { get; internal set; }
        public DateTime? DequeuedTime { get; internal set; }
        public DateTime? CompletedTime { get; internal set; }
        public TimeSpan? TimeToComplete { get; internal set; }
        public TimeSpan? TimeInQueue { get; internal set; }
        internal Stopwatch TimeInQueueCounter { get; set; }
        internal WorkQueuer WorkQueuer { get; set; }

        internal TaskCompletionSource<int> _tcsNotifyDequeued = new TaskCompletionSource<int>();

        TaskCompletionSource<int> _taskCompletionSource = new TaskCompletionSource<int>();
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
        public string StackTraceText { get; set; }

        public void Dispose() {
            if(!_disposed) {
                Cancellation.Dispose();
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

        public string SchedulingStackTrace { get; internal set; }

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
        public string[] EnqueuingContextStackTrace { get; set; }
        public decimal TimeInExecution { get; set; }

        public WorkJobExecutionStat(WorkJobExecutionRequest x) {
            Description = x.WorkJob.Name;
            EnqueuedAt = x.EnqueuedTime;
            StartedAt = x.DequeuedTime;
            EnqueuingContextStackTrace = x.StackTrace.ToString()
                .Split('\n')
                .SkipWhile(x => x.Contains("Figlotech.Core.WorkQueuer"))
                .Select(x => x?.Trim())
                .Where(x => !string.IsNullOrEmpty(x))
                .ToArray();
            TimeWaiting = (decimal)((x.DequeuedTime ?? DateTime.UtcNow) - (x.EnqueuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
            TimeInExecution = (decimal)(DateTime.UtcNow - (x.DequeuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
        }
    }

    public sealed class WorkQueuer : IDisposable {
        public static int qid_increment = 0;
        private int QID = ++qid_increment;
        public String Name;

        public event Func<WorkJobExecutionRequest, Task> OnWorkEnqueued;
        public event Func<WorkJobExecutionRequest, Task> OnWorkDequeued;
        public event Func<WorkJobExecutionRequest, Task> OnWorkComplete;
        public event Func<WorkJobExecutionRequest, Exception, Exception, Task> OnExceptionInHandler;

        Timer KeepAliveTimer;

        Queue<WorkJobExecutionRequest> WorkQueue = new Queue<WorkJobExecutionRequest>();
        List<WorkJobExecutionRequest> HeldJobs = new List<WorkJobExecutionRequest>();
        List<WorkJobExecutionRequest> ActiveJobs = new List<WorkJobExecutionRequest>();
        int NumActiveJobs {
            get {
                lock (ActiveJobs) {
                    return ActiveJobs.Count;
                }
            }
        }

        public decimal AverageTaskResolutionTime => WorkDone > 0 ? TotalTaskResolutionTime / WorkDone : 0;
        public decimal TotalTaskResolutionTime { get; private set; } = 1;
        public bool Active { get; private set; } = false;

        public static int DefaultSleepInterval = 50;

        public bool IsClosed { get { return closed; } }
        public bool IsRunning { get; private set; } = false;

        private bool isPaused = false;

        public WorkQueuer(String name, int maxThreads = -1, bool init_started = true) {
            if (maxThreads <= 0) {
                maxThreads = Environment.ProcessorCount - 1;
            }
            MaxParallelTasks = Math.Max(1, maxThreads);
            Name = name;
            if (init_started)
                Start();
        }

        public void Pause() {

        }

        public async Task Stop(bool wait = true) {
            Active = false;

            if (KeepAliveTimer != null) {
                KeepAliveTimer.Dispose();
            }
            if (wait) {
                WorkJobExecutionRequest peekJob = null;
                while (true) {
                    if (NumActiveJobs < this.MaxParallelTasks) {
                        SpawnWorker();
                    }
                    if (NumActiveJobs == 0 && TotalWork == WorkDone) {
                        break;
                    }
                    if (NumActiveJobs < Math.Min(this.MaxParallelTasks, WorkQueue.Count)) {
                        SpawnWorker();
                    }
                    lock(WorkQueue) {
                        if(WorkQueue.Count > 0) {
                            peekJob = WorkQueue.Peek();
                        }
                    }

                    await Task.Delay(500).ConfigureAwait(false);
                }
            }
            IsRunning = false;
        }

        public TimeSpan TimeIdle {
            get {
                if (WentIdle > DateTime.UtcNow) {
                    return TimeSpan.FromMilliseconds(0);
                }
                return (DateTime.UtcNow - WentIdle);
            }
        }

        bool closed = false;
        public int MaxParallelTasks { get; set; } = 0;
        bool inited = false;

        int workerIds = 1;

        public void Close() {
            closed = true;
        }

        public void Start() {
            if (Active || IsRunning)
                return;
            Active = true;
            IsRunning = true;
            lock (HeldJobs) {
                lock (WorkQueue) {
                    foreach (var job in HeldJobs) {
                        WorkQueue.Enqueue(job);
                    }
                }
                HeldJobs.Clear();
            }

            if (KeepAliveTimer != null) {
                KeepAliveTimer.Dispose();
            }
            // KeepAliveTimer = new Timer(_keepAlive, null, 6000, 6000);
            SpawnWorker();
        }

        public static async Task Live(Action<WorkQueuer> act, int parallelSize = -1) {
            if (parallelSize <= 0)
                parallelSize = Environment.ProcessorCount;
            using (var queuer = new WorkQueuer($"AnnonymousLiveQueuer", parallelSize)) {
                queuer.Start();
                act(queuer);
                await queuer.Stop(true).ConfigureAwait(false);
            }
        }
        public async Task AccompanyJob(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var wj = EnqueueTask(a, exceptionHandler, finished);
            await wj;
        }

        private DateTime WentIdle = DateTime.UtcNow;

        public int TotalWork { get; private set; } = 0;
        public int WorkDone { get; private set; } = 0;
        public int Executing { get; private set; } = 0;
        public int InQueue { get; private set; } = 0;
        public int Cancelled { get; private set; } = 0;


        private AtomicList<Task> Tasks = new AtomicList<Task>();

        public WorkJobExecutionStat[] ActiveTaskStat() {
            lock (WorkQueue) {
                return ActiveJobs.Select(x => new WorkJobExecutionStat(x))
                .ToArray();
            }
        }

        public WorkJobExecutionRequest Enqueue(WorkJob job, CancellationToken? requestCancellation = null) {
            var request = new WorkJobExecutionRequest(job, requestCancellation);

            request.EnqueuedTime = DateTime.UtcNow;
            request.Status = WorkJobRequestStatus.Queued;
            request.StackTrace = new System.Diagnostics.StackTrace();
            request.StackTraceText = request.StackTrace.ToString();
            request.TimeInQueueCounter = new Stopwatch();
            request.TimeInQueueCounter.Start();
            request.WorkQueuer = this;
            if (Active) {
                lock (WorkQueue) {
                    WorkQueue.Enqueue(request);
                    InQueue++;
                    TotalWork++;
                }
            } else {
                lock (HeldJobs) {
                    HeldJobs.Add(request);
                }
            }
            this.OnWorkEnqueued?.Invoke(request);
            SpawnWorker();
            return request;
        }

        object selfLockSpawnWorker2 = new object();
        int i;

        private void _keepAlive(object ctx) {
            lock (ActiveJobs) {
                for (int i = ActiveJobs.Count - 1; i >= 0; i--) {
                    if (ActiveJobs[i].WorkJob.ActionTask != null && (
                        ActiveJobs[i].WorkJob.ActionTask.IsFaulted || ActiveJobs[i].WorkJob.ActionTask.IsCanceled)
                    ) {
                        ActiveJobs.RemoveAt(i);
                    }
                }
            }
            SpawnWorker();
        }

        private bool SpawnWorker() {
            WorkJobExecutionRequest job = null;
            lock (selfLockSpawnWorker2) {
                lock (WorkQueue) {
                    do {
                        if (NumActiveJobs < this.MaxParallelTasks && WorkQueue.Count > 0) {
                            job = WorkQueue.Dequeue();
                            InQueue--;
                            if(job.Cancellation.IsCancellationRequested) {
                                Cancelled++;
                                WorkDone++;
                                job.Cancellation.Dispose();
                                continue;
                            }
                            lock (Tasks) {
                                Tasks.Add(job.TaskCompletionSource.Task);
                            }
                            lock (ActiveJobs) {
                                ActiveJobs.Add(job);
                            }
                            break;
                        }
                    } while(job != null);
                }
                if (job != null) {
                    return ThreadPool.UnsafeQueueUserWorkItem(async _ => {
                        job.TimeInQueueCounter.Stop();
                        job.TimeInQueue = TimeSpan.FromMilliseconds(job.TimeInQueueCounter.ElapsedMilliseconds);
                        this.OnWorkDequeued?.Invoke(job);
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        var thisWorkerId = workerIds++;
                        Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} started");
                        Exception exception = null;
                        Executing++;
                        try {
                            job.DequeuedTime = DateTime.UtcNow;
                            job._tcsNotifyDequeued.TrySetResult(0);
                            job.Status = WorkJobRequestStatus.Running;
                            if (job.WorkJob.action != null) {
                                using(var womboCombo = CancellationTokenSource.CreateLinkedTokenSource(job.Cancellation.Token, job.RequestCancellation)) {
                                    job.WorkJob.ActionTask = job.WorkJob.action(job.Cancellation.Token);
                                }
                                await job.WorkJob.ActionTask.ConfigureAwait(false);
                                job.Status = WorkJobRequestStatus.Finished;
                            }
                            if (job.WorkJob.finished != null) {
                                try {
                                    await job.WorkJob.finished(true).ConfigureAwait(false);
                                } catch (Exception ex) {
                                    var wrappedException = new WorkJobException("Error Executing WorkJob", job, ex);
                                    Fi.Tech.Throw(wrappedException);
                                }
                            }
                            Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} executed OK");
                        } catch (Exception x) {
                            job.Status = WorkJobRequestStatus.Failed;
                            var wrappedException = new WorkJobException("Error Executing WorkJob", job, x);
                            if (job.WorkJob.handling != null) {
                                try {
                                    await job.WorkJob.handling(
                                        wrappedException
                                    ).ConfigureAwait(false);
                                } catch (Exception ex) {
                                    exception = ex;
                                    try {
                                        var handlerTask = this.OnExceptionInHandler?.Invoke(job, x, ex);
                                        if(handlerTask is Task) {
                                            await handlerTask.ConfigureAwait(false);
                                        }
                                    } catch(Exception exx) {
                                        Fi.Tech.Throw(new AggregateException("User code generated exception in the hander AND in the handler of the handler.", x, ex, exx));
                                    }
                                }
                            } else {
                                Fi.Tech.Throw(wrappedException);
                            }
                            if (job.WorkJob.finished != null) {
                                try {
                                    await job.WorkJob.finished(false).ConfigureAwait(false);
                                } catch (Exception ex) {
                                    var wrappedException2 = new WorkJobException("Error Executing WorkJob", job,
                                        new AggregateException(x, ex)
                                    );
                                    Fi.Tech.Throw(wrappedException2);
                                }
                            }
                            Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} thrown an Exception: {x.Message}");
                        } finally {
                            try {
                                sw.Stop();
                                lock (this) {
                                    job.CompletedTime = DateTime.UtcNow;
                                    job.TimeToComplete = sw.Elapsed;
                                    job.Status = WorkJobRequestStatus.Finished;
                                    this.OnWorkComplete?.Invoke(job);
                                    WorkDone++;
                                    if(job.Cancellation.IsCancellationRequested) {
                                        Cancelled++;
                                    }
                                    job.Cancellation.Dispose();
                                    Executing--;
                                    job.TimeToComplete = sw.Elapsed;
                                    if (exception != null) {
                                        job.TaskCompletionSource.SetException(exception);
                                        _ = job.TaskCompletionSource.Task.Exception;
                                    } else {
                                        job.TaskCompletionSource.SetResult(0);
                                    }
                                    this.TotalTaskResolutionTime += (decimal)sw.Elapsed.TotalMilliseconds;

                                    lock (ActiveJobs) {
                                        ActiveJobs.Remove(job);
                                    }
                                    lock (Tasks) {
                                        Tasks.Remove(job.TaskCompletionSource.Task);
                                    }
                                    SpawnWorker();
                                }
                            } catch (Exception x) {
                                Debugger.Break();
                            }
                            Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} cleanup OK");
                        }
                    }, null);
                } else {
                    return false;
                }
            }
        }

        public void Enqueue(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            var t = Enqueue(retv);
        }
        public WorkJobExecutionRequest EnqueueTask(Func<CancellationToken, ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            return Enqueue(retv);
        }
        public WorkJobExecutionRequest EnqueueTask(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            return Enqueue(retv);
        }

        public void Dispose() {
            Stop(true).Wait();
        }
    }
}
