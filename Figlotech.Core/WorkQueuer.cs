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

    public class JobProgress {
        public String Status;
        public int TotalSteps;
        public int CompletedSteps;
    }

    public class WorkJob {
        public int id = ++idGen;
        private static int idGen = 0;
        internal Thread AssignedThread = null;
        public Func<ValueTask> action;
        public Func<bool, ValueTask> finished;
        public Func<Exception, ValueTask> handling;
        public WorkJobStatus status;
        public DateTime? EqueuedTime = DateTime.Now;
        public DateTime? DequeuedTime;
        public DateTime? CompletedTime;
        public TimeSpan? CompletionTime { get; internal set; }
        public String Name { get; set; } = null;

        TaskCompletionSource<int> _taskCompletionSource = new TaskCompletionSource<int>();
        public TaskCompletionSource<int> TaskCompletionSource => _taskCompletionSource;

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

    public sealed class WorkQueuer : IDisposable {
        public static int qid_increment = 0;
        private int QID = ++qid_increment;
        public String Name;

        private Thread _supervisor;

        Queue<WorkJob> WorkQueue = new Queue<WorkJob>();
        List<WorkJob> HeldJobs = new List<WorkJob>();
        List<WorkJob> PendingOrExecutingJobs = new List<WorkJob>();
        List<WorkJob> ActiveJobs = new List<WorkJob>();

        public decimal AverageTaskResolutionTime => TotalTaskResolutionTime / WorkDone;
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
                WorkJob peekJob = null;
                while (true) {
                    if (ActiveJobs.Count < this.MaxParallelTasks && peekJob != null) {
                        SpawnWorker();
                    }
                    lock (PendingOrExecutingJobs) {
                        if (PendingOrExecutingJobs.Count > 0) {
                            peekJob = PendingOrExecutingJobs[0];
                        } else {
                            if(ActiveJobs.Count == 0) {
                                break;
                            }
                        }
                    }
                    if(peekJob != null && ActiveJobs.Count < Math.Min(this.MaxParallelTasks, WorkQueue.Count)) {
                        SpawnWorker();
                    }
                    if(peekJob != null && peekJob.status != WorkJobStatus.Finished) {
                        await peekJob;
                    } else {
                        lock (PendingOrExecutingJobs) {
                            PendingOrExecutingJobs.Remove(peekJob);
                        }
                        lock (ActiveJobs) {
                            ActiveJobs.Remove(peekJob);
                        }
                    }
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
            lock(HeldJobs) {
                lock(WorkQueue) {
                    foreach(var job in HeldJobs) {
                        WorkQueue.Enqueue(job);
                    }
                }
                lock (PendingOrExecutingJobs) {
                    PendingOrExecutingJobs.AddRange(HeldJobs);
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

        public int TotalWork = 0;
        public int WorkDone = 0;

        private List<Task> Tasks = new List<Task>();

        public WorkJob Enqueue(WorkJob job) {
            if (Active) {
                lock (WorkQueue) {
                    WorkQueue.Enqueue(job);
                }
                lock (PendingOrExecutingJobs) {
                    PendingOrExecutingJobs.Add(job);
                }
            } else {
                lock (HeldJobs) {
                    HeldJobs.Add(job);
                }
            }

            job.EqueuedTime = DateTime.UtcNow;
            job.status = WorkJobStatus.Queued;

            SpawnWorker();
            TotalWork++;

            return job;
        }

        object selfLockSpawnWorker2 = new object();
        int i;
        private void SpawnWorker() {
            ThreadPool.UnsafeQueueUserWorkItem(async _ =>
            {
                WorkJob job = null;
                lock(selfLockSpawnWorker2) {
                    lock(WorkQueue) {
                        lock(ActiveJobs) {
                            ActiveJobs.RemoveAll(x => x.status == WorkJobStatus.Finished);
                            if(ActiveJobs.Count < this.MaxParallelTasks && WorkQueue.Count > 0) {
                                job = WorkQueue.Dequeue(); 
                                lock (Tasks) {
                                    Tasks.Add(job.TaskCompletionSource.Task);
                                }
                                ActiveJobs.Add(job);
                            }
                        }
                    }
                }
                if(job?.status == WorkJobStatus.Finished) {
                    Debugger.Break();
                }
                if(job != null) {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    var thisWorkerId = workerIds++;
                    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", ()=> $"Worker {thisWorkerId} started");
                    try {
                        job.DequeuedTime = DateTime.UtcNow;
                        job.status = WorkJobStatus.Running;
                        await job.action().ConfigureAwait(false);
                        if (job.finished != null) {
                            await job.finished(true).ConfigureAwait(false);
                        }
                        Fi.Tech.WriteLineInternal("FTH:WorkQueuer", ()=> $"Worker {thisWorkerId} executed OK");
                    } catch (Exception x) {
                        if (job.handling != null) {
                            await job.handling(x).ConfigureAwait(false);
                        } else {
                            Fi.Tech.Throw(x);
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
                                job.TaskCompletionSource.SetResult(0);
                                WorkDone++;
                                job.CompletionTime = TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds);
                                this.TotalTaskResolutionTime += (decimal)sw.Elapsed.TotalMilliseconds;
                                lock (ActiveJobs) {
                                    if (ActiveJobs.Contains(job)) {
                                        if (!ActiveJobs.Remove(job)) {
                                            Debugger.Break();
                                        }
                                    }
                                }
                                lock (PendingOrExecutingJobs) {
                                    if (PendingOrExecutingJobs.Contains(job)) {
                                        if (!PendingOrExecutingJobs.Remove(job)) {
                                            Debugger.Break();
                                        }
                                    }
                                }
                                lock (Tasks) {
                                    Tasks.Remove(job.TaskCompletionSource.Task);
                                }
                                if (WorkQueue.Count > 0) {
                                    SpawnWorker();
                                }
                            }
                        } catch(Exception x) {
                            Debugger.Break();
                        }
                        Fi.Tech.WriteLineInternal("FTH:WorkQueuer", () => $"Worker {thisWorkerId} cleanup OK");
                    }
                }
            }, null);
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
