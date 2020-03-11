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
        public String Name { get; set; } = null;

        public TaskCompletionSource<int> TaskCompletionSource { get; set; } = new TaskCompletionSource<int>();

        public TaskAwaiter<int> GetAwaiter() {
            return TaskCompletionSource.Task.GetAwaiter();
        }

        public void OnCompleted(Action continuation) {
            throw new NotImplementedException();
        }

        //#if DEBUG
        //        public StackFrame[] ContextStack;
        //#endif

        public WorkJob(Func<ValueTask> method, Func<Exception, ValueTask> errorHandling, Func<bool, ValueTask> actionWhenFinished) {
            action = method;
            finished = actionWhenFinished;
            handling = errorHandling;
            status = WorkJobStatus.Queued;

            //#if DEBUG
            //            StackTrace stackTrace = new StackTrace();   // get call stack
            //            ContextStack = stackTrace.GetFrames();      // get method calls (frames)
            //#endif
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
        List<Thread> detectedLongWorkThreads = new List<Thread>();
        decimal avgTaskResolutionTime = 1;

        decimal totTaskResolutionTime = 1;

        decimal totTasksResolved = 0;
        ManualResetEvent QueueResetEvent { get; set; } = new ManualResetEvent(false);

        public int GcInterval = 5000;

        private bool _active = false;
        public bool Active {
            get {
                return _active;
            }
            private set {
                _active = value;
            }
        }
        private bool isRunning = false;
        public static int DefaultSleepInterval = 50;

        public bool IsClosed { get { return closed; } }
        public bool IsRunning { get { return isRunning; } }

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
                //_supervisor.Join();
                //while (workers.Count > 0) {
                //    if(workers[workers.Count - 1].ManagedThreadId == Thread.CurrentThread.ManagedThreadId) {
                //        workers.RemoveAt(workers.Count - 1);
                //        continue;
                //    }
                //    if(workers[workers.Count-1].IsAlive) {
                //        workers[workers.Count - 1].Join();
                //    }
                //}
                //workers.Clear();
                WorkJob peekJob = null;
                while (true) {
                    lock (PendingOrExecutingJobs) {
                        if (PendingOrExecutingJobs.Count > 0) {
                            peekJob = PendingOrExecutingJobs[0];
                        } else {
                            break;
                        }
                        if(ActiveJobs.Count < this.MaxParallelTasks && peekJob != null) {
                            SpawnWorker2();
                        }
                    }
                    if(peekJob != null) {
                        await peekJob;
                    }
                }
            }
            isRunning = false;
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

        //private void SupervisorJob() {
        //    int wqc = 0;
        //    int numWorkers = 0;
        //    var clearHolds = DateTime.UtcNow;
        //    var lastSupervisorRun = DateTime.UtcNow;
        //    while (Active || wqc > 0) {
        //        lock (workers) {
        //            numWorkers = workers.Count;
        //            workers.RemoveAll(t => t.ThreadState == ThreadState.Stopped);
        //            workers.RemoveAll(t => t.ThreadState == ThreadState.Aborted);
        //            lock (workQueue) {
        //                wqc = workQueue.Count;
        //                if (wqc > 0) {
        //                    lastSupervisorRun = DateTime.UtcNow;
        //                }
        //            }
        //            if (wqc > workers.Count && workers.Count < parallelSize) {
        //                SpawnWorker();
        //            }
        //            if (DateTime.UtcNow.Subtract(clearHolds) > TimeSpan.FromMilliseconds(GcInterval)) {
        //                lock (holdJobs) {
        //                    holdJobs.Clear();
        //                }
        //                clearHolds = DateTime.UtcNow;
        //            }
        //            if (DateTime.UtcNow.Subtract(lastSupervisorRun) > TimeSpan.FromMilliseconds(ExtraWorkerTimeout)) {
        //                return;
        //            }
        //        }
        //        Thread.Sleep(DefaultSleepInterval);
        //    }
        //}

        public int MaxParallelTasks { get; set; } = 0;
        bool inited = false;

        int workerIds = 1;

        public void Close() {
            closed = true;
        }

        public void Start() {
            if (Active || isRunning)
                return;
            Active = true;
            //while(workers.Count < parallelSize) {
            //    workers.Add(SpawnWorker());
            //}
            isRunning = true;
            lock(HeldJobs) {
                lock(WorkQueue) {
                    lock(PendingOrExecutingJobs) {
                        WorkQueue.EnqueueRange(HeldJobs);
                        PendingOrExecutingJobs.AddRange(HeldJobs);
                        HeldJobs.Clear();
                    }
                }
            }

            SpawnWorker2();
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
            //bool shouldForceSpawnWorker = false;
            if (Active) {
                lock (WorkQueue) {
                    lock (PendingOrExecutingJobs) {
                        WorkQueue.Enqueue(job);
                        PendingOrExecutingJobs.Add(job);
                    }
                }
            } else {
                lock (HeldJobs) {
                    HeldJobs.Add(job);
                }
            }

            //SpawnWorker(shouldForceSpawnWorker);
            job.EqueuedTime = DateTime.UtcNow;
            job.status = WorkJobStatus.Queued;

            SpawnWorker2();
            QueueResetEvent.Set();
            TotalWork++;

            return job;
        }

        object selfLockSpawnWorker2 = new object();
        int i;
        private void SpawnWorker2() {
            ThreadPool.UnsafeQueueUserWorkItem(async _ =>
            {
                WorkJob job = null;
                lock(selfLockSpawnWorker2) {
                    lock(WorkQueue) {
                        lock(ActiveJobs) {
                            ActiveJobs.RemoveAll(x => x.CompletedTime != null);
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
                if(job != null) {
                    try {
                        job.DequeuedTime = DateTime.UtcNow;
                        job.status = WorkJobStatus.Running;
                        await job.action().ConfigureAwait(false);
                        if (job.finished != null) {
                            await job.finished(true).ConfigureAwait(false);
                        }
                    } catch (Exception x) {
                        if (job.handling != null) {
                            await job.handling(x).ConfigureAwait(false);
                        } else {
                            Fi.Tech.Throw(x);
                        }
                        if (job.finished != null) {
                            await job.finished(false).ConfigureAwait(false);
                        }
                    } finally {
                        job.CompletedTime = DateTime.UtcNow;
                        job.status = WorkJobStatus.Finished;
                        WorkDone++;
                        lock (PendingOrExecutingJobs) {
                            PendingOrExecutingJobs.Remove(job);
                        }
                        lock (Tasks) {
                            Tasks.Remove(job.TaskCompletionSource.Task);
                        }
                        job.TaskCompletionSource.SetResult(0);
                        SpawnWorker2();
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
