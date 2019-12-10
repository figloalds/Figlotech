using System;
using System.Collections.Generic;
using System.Linq;
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
        public Func<Task> action;
        public Task TaskObject { get; set; }
        public Func<bool, Task> finished;
        public Func<Exception, Task> handling;
        public WorkJobStatus status;
        public DateTime? enqueued = DateTime.Now;
        public DateTime? dequeued;
        public DateTime? completed;
        public EventWaitHandle WaitHandle { get; private set; } = new EventWaitHandle(false, EventResetMode.ManualReset);
        public String Name { get; set; } = null;

        public void Await() {
            while (completed == null && AssignedThread != Thread.CurrentThread) {
                WaitHandle.WaitOne();
            }
        }

        public async Task Conclusion() {
            if(TaskObject.Status != TaskStatus.RanToCompletion) {
                await this.TaskObject;
            }
        }

        //#if DEBUG
        //        public StackFrame[] ContextStack;
        //#endif

        public WorkJob(Func<Task> method, Func<Exception, Task> errorHandling, Func<bool, Task> actionWhenFinished) {
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

    public class WorkQueuer : IDisposable {
        public static int qid_increment = 0;
        private int QID = ++qid_increment;
        public String Name;

        private Thread _supervisor;

        Queue<WorkJob> WorkQueue = new Queue<WorkJob>();
        List<WorkJob> HeldJobs = new List<WorkJob>();
        List<WorkJob> ActiveJobs = new List<WorkJob>();
        List<Thread> workers = new List<Thread>();
        List<Thread> detectedLongWorkThreads = new List<Thread>();
        decimal avgTaskResolutionTime = 1;

        decimal totTaskResolutionTime = 1;

        decimal totTasksResolved = 0;
        ManualResetEvent QueueResetEvent { get; set; } = new ManualResetEvent(false);

        public int GcInterval = 5000;

        private bool _active = false;
        public bool Active {
            get {
                if (FiTechCoreExtensions.MainThreadHandler != null) {
                    return _active && (FiTechCoreExtensions.MainThreadHandler.ThreadState == ThreadState.Running);
                }
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

        public int ParallelSize => MinWorkers + ExtraWorkers;

        ThreadPriority _defaultWorkerPriority = ThreadPriority.Normal;
        public ThreadPriority DefaultWorkerPriority {
            get {
                return _defaultWorkerPriority;
            }
            set {
                _defaultWorkerPriority = value;
                foreach (var a in workers)
                    a.Priority = value;
            }
        }

        public WorkQueuer(String name, int maxThreads = -1, bool init_started = true) {
            if (maxThreads <= 0) {
                maxThreads = Environment.ProcessorCount - 1;
            }
            MinWorkers = Math.Max(1, maxThreads);
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
                    lock (WorkQueue) {
                        if (WorkQueue.Count > 0) {
                            peekJob = WorkQueue.Peek();
                        } else {
                            break;
                        }
                    }
                    await peekJob.Conclusion();
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

        public void SetThreadsPriority(ThreadPriority priority) {
            foreach (var a in workers) {
                a.Priority = priority;
            }
        }

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

        public int ExtraWorkers { get; set; } = Math.Max(1, Environment.ProcessorCount - 1);
        public int ExtraWorkerTimeout { get; set; } = 7000;
        public int MainWorkerTimeout { get; set; } = 60000;
        public int MinWorkers { get; set; } = 0;
        bool inited = false;
        private void SpawnWorker(bool force = false) {
            if (inited && !force) {
                return;
            }
            inited = true;
            lock (workers) {
                lock (detectedLongWorkThreads) {
                    WorkJob dlw = null;
                    detectedLongWorkThreads.RemoveAll(t => !t.IsAlive);
                    if (detectedLongWorkThreads.Count < ExtraWorkers) {
                        lock (ActiveJobs) {
                            ActiveJobs.FirstOrDefault(j => j.dequeued != null && j.completed == null && DateTime.UtcNow.Subtract(j.dequeued.Value) > TimeSpan.FromMilliseconds(ExtraWorkerTimeout * 2));
                            if (dlw != null) {
                                Fi.Tech.WriteLine("FTH:WorkQueuer", $"{dlw.Name}({dlw.id}) has been running for too long, removing from main queuer");
                                workers.Remove(dlw.AssignedThread);
                                detectedLongWorkThreads.Add(dlw.AssignedThread);
                            } else {
                                var tpm = avgTaskResolutionTime > 0 ? 60 * 1000 / avgTaskResolutionTime : 0;
                                Fi.Tech.WriteLine("FTH:WorkQueuer", $"{Name}::{QID} worker overload {WorkQueue.Count} tasks to complete, {workers.Count + detectedLongWorkThreads.Count} threads resolving average {tpm.ToString("0.00")}tpm");
                            }
                        }
                    }
                }

                if (workers.Count >= ParallelSize) {
                    return;
                }
                DateTime lastJobProcessedStamp = DateTime.UtcNow;
                var CurrentCulture = Thread.CurrentThread.CurrentCulture;
                var CurrentUICulture = Thread.CurrentThread.CurrentUICulture;
                Thread workerThread = Fi.Tech.SafeCreateThread(() => {
                    Thread.CurrentThread.CurrentCulture = CurrentCulture;
                    Thread.CurrentThread.CurrentUICulture = CurrentCulture;
                    //lock (workers) {
                    //    workers.Add(Thread.CurrentThread);
                    //}
                    WorkJob job;

                    int foulEntryCount = 2;
                    bool isFoulEntry = false;
                    while (true) {
                        job = null;
                        lock(workers) {
                            if(workers.Count > ParallelSize) {
                                break;
                            }
                        }
                        lock (WorkQueue) {
                            if (WorkQueue.Count > workers.Count && workers.Count < ParallelSize) {
                                SpawnWorker(true);
                            }
                            if (WorkQueue.Count > 0) {
                                job = WorkQueue.Dequeue();
                                Thread.CurrentThread.IsBackground = false;
                                lock (ActiveJobs)
                                    ActiveJobs.Add(job);
                            }
                        }
                        if (job == null) {
                            if (isFoulEntry && --foulEntryCount < 0) {
                                break;
                            }
                            Thread.CurrentThread.IsBackground = true;
                            Thread.Sleep(200);
                            var cnt = 0;
                            lock (WorkQueue) {
                                cnt = WorkQueue.Count;
                            }
                            if (cnt < 1) {
                                isFoulEntry = true;
                                var timeout = workers.Count > MinWorkers ? ExtraWorkerTimeout : MainWorkerTimeout;
                                QueueResetEvent.WaitOne(timeout);
                            }
                            continue;
                        }
                        isFoulEntry = false;
                        foulEntryCount = 10;
                        List<Exception> exes = new List<Exception>();

                        Thread.CurrentThread.IsBackground = false;

                        job.dequeued = DateTime.Now;
                        job.AssignedThread = Thread.CurrentThread;
                        Fi.Tech.WriteLine("FTH:WorkQueuer", $"[{Thread.CurrentThread.Name}] Job {Name}:{job.id} dequeued for execution after {(job.dequeued.Value - job.enqueued.Value).TotalMilliseconds}ms");
                        var callPoint = job?.action?.Method.DeclaringType?.DeclaringType?.Name;

                        try {
                            job?.action?.Invoke();
                            job.status = WorkJobStatus.Finished;
                            job?.finished?.Invoke(true);
                            job.completed = DateTime.Now;
                            job.AssignedThread = null;
                            Fi.Tech.WriteLine("FTH:WorkQueuer", $"[{Thread.CurrentThread.Name}] Job {Name}@{callPoint}:{job.id} finished in {(job.completed.Value - job.dequeued.Value).TotalMilliseconds}ms");
                        } catch (Exception x) {
                            try {
                                job.completed = DateTime.Now;
                                Fi.Tech.WriteLine("FTH:WorkQueuer", $"[{Thread.CurrentThread.Name}] Job {Name}@{callPoint}:{job.id} failed in {(job.completed.Value - job.dequeued.Value).TotalMilliseconds}ms with message: {x.Message}");
                                //var callPoint = job?.action?.Method.DeclaringType?.Name;
                                var jobdescription = $"{job.Name ?? "annonymous_job"}::{job.id}";
                                var msg = $"Error executing WorkJob {Name}/{jobdescription}@{callPoint}: {x.Message}";
                                try {
                                    job?.handling?.Invoke(new Exception(msg, x));
                                } catch (Exception y) {
                                    Fi.Tech.Throw(y);
                                }
                            } catch(Exception z) {
                                System.Diagnostics.Debugger.Break();
                            }
                            try {
                                job?.finished?.Invoke(false);
                            } catch(Exception ex) {
                                Fi.Tech.Throw(ex);
                            }
                        }
                        job.status = WorkJobStatus.Finished;
                        totTasksResolved++;
                        WorkDone++;
                        var thisTaskResolutionTime = (decimal)(job.completed.Value - job.enqueued.Value).TotalMilliseconds;
                        avgTaskResolutionTime -= avgTaskResolutionTime / totTasksResolved;
                        avgTaskResolutionTime += thisTaskResolutionTime / totTasksResolved;
                        job.WaitHandle.Set();
                        lock (ActiveJobs) ActiveJobs.Remove(job);
                        lastJobProcessedStamp = DateTime.UtcNow;
                        job = null;
                    }
                    lock (workers) {
                        workers.Remove(Thread.CurrentThread);
                        if (workers.Count == 0) {
                            inited = false;
                        }
                    }
                });
                workerThread.Priority = DefaultWorkerPriority;
                workerThread.Name = $"FTWQ_{Name}_Worker_{workerIds++}";
                workers.Add(workerThread);
                workerThread.Start();
            }
        }
        int workerIds = 1;

        public void Close() {
            closed = true;
        }

        public void Start() {
            if (Active || isRunning)
                return;
            Active = true;
            workers.RemoveAll(w => w.ThreadState == ThreadState.Aborted);
            workers.RemoveAll(w => w.ThreadState == ThreadState.Stopped);
            //while(workers.Count < parallelSize) {
            //    workers.Add(SpawnWorker());
            //}
            isRunning = true;
            WorkQueue.EnqueueRange(HeldJobs);
            HeldJobs.Clear();
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
        public async Task AccompanyJob(Func<Task> a, Func<Exception, Task> exceptionHandler = null, Func<bool, Task> finished = null) {
            var wj = Enqueue(a, exceptionHandler, finished);
            await wj.Conclusion();
        }

        private DateTime WentIdle = DateTime.UtcNow;

        public int TotalWork = 0;
        public int WorkDone = 0;

        private List<Task> Tasks = new List<Task>();

        public WorkJob Enqueue(WorkJob job) {
            //bool shouldForceSpawnWorker = false;
            if (Active) {
                lock (WorkQueue) {
                    WorkQueue.Enqueue(job);
                }
            } else {
                HeldJobs.Add(job);
            }

            //SpawnWorker(shouldForceSpawnWorker);
            SpawnWorker2();
            QueueResetEvent.Set();
            TotalWork++;

            return job;
        }
        object selfLockSpawnWorker2 = new object();
        private void SpawnWorker2() {
            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                lock(selfLockSpawnWorker2) {
                    lock(WorkQueue) {
                        lock(ActiveJobs) {
                            ActiveJobs.RemoveAll(x => x.completed != null);
                            while(ActiveJobs.Count < this.MinWorkers && WorkQueue.Count > 0) {
                                var job = WorkQueue.Dequeue();
                                var task = Fi.Tech.FireTask(async()=> {
                                    try {
                                        await job.action();
                                        if(job.finished != null) {
                                            await job.finished(true);
                                        }
                                    } catch(Exception x) {
                                        if(job.handling != null) {
                                            await job.handling(x);
                                        } else {
                                            Fi.Tech.Throw(x);
                                        }
                                        if (job.finished != null) {
                                            await job.finished(false);
                                        }
                                    } finally {
                                        job.completed = DateTime.UtcNow;
                                        job.status = WorkJobStatus.Finished;
                                        SpawnWorker2();
                                    }
                                });
                                job.TaskObject = task;
                                Tasks.Add(task);
                                ActiveJobs.Add(job);
                            }
                        }
                    }
                }
            }, null);
        }

        public WorkJob Enqueue(Func<Task> a, Func<Exception, Task> exceptionHandler = null, Func<bool, Task> finished = null) {
            var retv = new WorkJob(a, exceptionHandler, finished);
            return Enqueue(retv);
        }
        
        public void Dispose() {
            while (this.workers.Count > 0) {
                Start();
                Stop(true);
            }
        }
    }
}
