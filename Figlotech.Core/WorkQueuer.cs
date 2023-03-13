using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public enum WorkJobStatus {
        Queued,
        Running,
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
        public WorkJobException(string message, WorkJob job, Exception inner) : base(message, inner) {
            this.EnqueuingContextStackTrace = job.StackTrace.ToString().Split('\n').Select(x => x?.Trim()).ToArray();
        }
    }

    public sealed class WorkJob {
        public int id = ++idGen;
        private static int idGen = 0;
        internal Thread AssignedThread = null;
        public string Description;
        public Func<ValueTask> action;
        public Func<bool, ValueTask> finished;
        public Func<Exception, ValueTask> handling;
        public WorkJobStatus status;
        public DateTime? EnqueuedTime = DateTime.Now;
        public DateTime? DequeuedTime;
        public DateTime? CompletedTime;
        public TimeSpan? CompletionTime { get; internal set; }
        public TimeSpan? TimeInQueue { get; internal set; }
        internal Stopwatch TimeInQueueCounter { get; set; }
        public String Name { get; set; } = null;

        TaskCompletionSource<int> _taskCompletionSource = new TaskCompletionSource<int>();
        public TaskCompletionSource<int> TaskCompletionSource => _taskCompletionSource;

        public StackTrace StackTrace { get; internal set; }

        public TaskAwaiter<int> GetAwaiter() {
            return TaskCompletionSource.Task.GetAwaiter();
        }

        public void OnCompleted(Action continuation) {
            throw new NotImplementedException();
        }

        public WorkJob(Func<ValueTask> method, Func<Exception, ValueTask> errorHandling, Func<bool, ValueTask> actionWhenFinished) {
            action = method;
            finished = actionWhenFinished;
            handling = errorHandling;
            status = WorkJobStatus.Queued;
        }
    }

    public sealed class WorkJobExecutionStat {
        public string Description { get; set; }
        public DateTime? EnqueuedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public decimal TimeWaiting { get; set; }
        public string[] EnqueuingContextStackTrace { get; set; }
        public decimal TimeInExecution { get; set; }

        public WorkJobExecutionStat(WorkJob x) {
            Description = x.Description;
            EnqueuedAt = x.EnqueuedTime;
            StartedAt = x.DequeuedTime;
            EnqueuingContextStackTrace = x.StackTrace.ToString()
                .Split('\n')
                .SkipWhile(x=> x.Contains("iglotech.Core.WorkQueuer"))
                .Select(x => x?.Trim())
                .Where(x=> !string.IsNullOrEmpty(x))
                .ToArray();
            TimeWaiting = (decimal)((x.DequeuedTime ?? DateTime.UtcNow) - (x.EnqueuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
            TimeInExecution = (decimal)(DateTime.UtcNow - (x.DequeuedTime ?? DateTime.UtcNow)).TotalMilliseconds;
        }
    }
    
    public sealed class WorkQueuer : IDisposable {
        public static int qid_increment = 0;
        private int QID = ++qid_increment;
        public String Name;

        private Thread _supervisor;

        public event Action<WorkJob> OnWorkEnqueued;
        public event Action<WorkJob> OnWorkDequeued;
        public event Action<WorkJob> OnWorkComplete;

        Queue<WorkJob> WorkQueue = new Queue<WorkJob>();
        List<WorkJob> HeldJobs = new List<WorkJob>();
        List<WorkJob> ActiveJobs = new List<WorkJob>();
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
            if (wait) {
                await Fi.Tech.FireTask(async () => {
                    WorkJob peekJob = null;
                    while (true) {
                        if (NumActiveJobs < this.MaxParallelTasks && peekJob != null) {
                            SpawnWorker();
                        }
                        if (NumActiveJobs == 0 && TotalWork == WorkDone) {
                            break;
                        }
                        if (peekJob != null && NumActiveJobs < Math.Min(this.MaxParallelTasks, WorkQueue.Count)) {
                            SpawnWorker();
                        }
                        if (peekJob != null) {
                            switch (peekJob.status) {
                                case WorkJobStatus.Running:
                                    await peekJob.TaskCompletionSource.Task.ConfigureAwait(false);
                                    break;
                                default:
                                    await Task.Delay(200).ConfigureAwait(false);
                                    break;
                            }
                        }
                    }
                }).TaskCompletionSource.Task.ConfigureAwait(false);
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

            SpawnWorker();
        }

        public static async Task Live(Action<WorkQueuer> act, int parallelSize = -1) {
            if (parallelSize <= 0)
                parallelSize = Environment.ProcessorCount;
            using (var queuer = new WorkQueuer($"AnnonymousLiveQueuer", parallelSize)) {
                queuer.Start();
                act(queuer);
                await queuer.Stop(true);
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


        private List<Task> Tasks = new List<Task>();
                
        public WorkJobExecutionStat[] ActiveTaskStat() {
            lock (WorkQueue) {
                return ActiveJobs.Select(x => new WorkJobExecutionStat(x))
                .ToArray();
            }
        }

        public WorkJob Enqueue(WorkJob job) {
            if (Active) {
                lock (WorkQueue) {
                    WorkQueue.Enqueue(job);
                    job.EnqueuedTime = DateTime.UtcNow;
                    job.status = WorkJobStatus.Queued;
                    job.StackTrace = new System.Diagnostics.StackTrace();
                    job.TimeInQueueCounter = new Stopwatch();
                    job.TimeInQueueCounter.Start();
                }
            } else {
                lock (HeldJobs) {
                    HeldJobs.Add(job);
                }
            }
            InQueue++;
            TotalWork++;
            this.OnWorkEnqueued?.Invoke(job);
            SpawnWorker();
            return job;
        }

        object selfLockSpawnWorker2 = new object();
        int i;

        private bool SpawnWorker() {
            WorkJob job = null;
            lock (selfLockSpawnWorker2) {
                lock (WorkQueue) {
                    if (NumActiveJobs < this.MaxParallelTasks && WorkQueue.Count > 0) {
                        job = WorkQueue.Dequeue();
                        InQueue--;
                        lock (Tasks) {
                            Tasks.Add(job.TaskCompletionSource.Task);
                        }
                    }
                }
            }
            if (job?.status == WorkJobStatus.Finished) {
                Debugger.Break();
            }
            if (job != null) {
                return ThreadPool.UnsafeQueueUserWorkItem(async _ => {
                    lock (ActiveJobs) {
                        Executing++;
                        ActiveJobs.Add(job);
                    }
                    job.TimeInQueueCounter.Stop();
                    job.TimeInQueue = TimeSpan.FromMilliseconds(job.TimeInQueueCounter.ElapsedMilliseconds);
                    this.OnWorkDequeued?.Invoke(job);
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    var thisWorkerId = workerIds++;
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} started");
                    Exception exception = null;
                    try {
                        job.DequeuedTime = DateTime.UtcNow;
                        job.status = WorkJobStatus.Running;
                        if (job.action != null) {
                            await job.action().ConfigureAwait(false);
                        }
                        if (job.finished != null) {
                            await job.finished(true).ConfigureAwait(false);
                        }
                        Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} executed OK");
                    } catch (Exception x) {
                        var wrappedException = new WorkJobException("Error Executing WorkJob", job, x);
                        if (job.handling != null) {
                            try {
                                await job.handling(
                                    wrappedException
                                ).ConfigureAwait(false);
                            } catch (Exception ex) {
                                exception = ex;
                            }
                        } else {
                            Fi.Tech.Throw(wrappedException);
                        }
                        if (job.finished != null) {
                            await job.finished(false).ConfigureAwait(false);
                        }
                        Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} thrown an Exception: {x.Message}");
                    } finally {
                        try {
                            sw.Stop();
                            lock (this) {
                                job.CompletedTime = DateTime.UtcNow;
                                job.status = WorkJobStatus.Finished;
                                WorkDone++;
                                Executing--;
                                job.CompletionTime = TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds);
                                this.OnWorkComplete?.Invoke(job);
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

        public void Enqueue(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            var t = Enqueue(retv);
        }
        public WorkJob EnqueueTask(Func<ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            return Enqueue(retv);
        }

        public void Dispose() {
            Stop(true).Wait();
        }
    }
}
