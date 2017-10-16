using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
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
        public Action action;
        public Action finished;
        public Action<Exception> handling;
        public WorkJobStatus status;
        public DateTime? enqueued = DateTime.Now;
        public DateTime? dequeued;
        public DateTime? completed;

        public void Await() {
            while (completed == null) {
                if (AssignedThread != null) {
                    lock (this) {
                        return;
                    }
                }
                Thread.Sleep(100);
            }
        }

        //#if DEBUG
        //        public StackFrame[] ContextStack;
        //#endif

        public WorkJob(Action method, Action actionWhenFinished, Action<Exception> errorHandling) {
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
    public class WorkQueuer : IContinuousExecutor, IDisposable {
        public static int qid_increment = 0;
        private int QID = ++qid_increment;
        public String Name;
        private Thread _supervisor;
        Queue<WorkJob> workQueue = new Queue<WorkJob>();
        List<Thread> workers = new List<Thread>();

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

        int parallelSize = 1;

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

        public static List<IContinuousExecutor> WorldQueuers = new List<IContinuousExecutor>();

        public WorkQueuer(String name, int maxThreads = -1, bool init_started = false) {
            if (maxThreads <= 0) {
                maxThreads = Environment.ProcessorCount - 1;
            }
            maxThreads = Math.Max(1, maxThreads);
            parallelSize = maxThreads;
            Name = name;
            WorldQueuers.Add(this);
            if (init_started)
                Start();
        }

        public static void StopAllQueuers() {
            Parallel.ForEach(WorldQueuers, a => {
                a.Stop(true);
            });
            //WorldQueuers.Clear();
        }

        public void Pause() {

        }

        public void Stop(bool wait = true) {
            Active = false;
            if (wait) {
                _supervisor.Join();
                foreach (var w in workers)
                    w.Join();
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

        private void SupervisorJob() {
            int wqc = 0;
            while (Active || wqc > 0) {
                lock(workers) {
                    workers.RemoveAll(t => t.ThreadState != ThreadState.Running);
                }
                lock (workQueue) {
                    wqc = workQueue.Count;
                }
                if (wqc > 0 && workers.Count < parallelSize) {
                    lock(workers) {
                        workers.Add(SpawnWorker());
                    }
                }
                Thread.Sleep(DefaultSleepInterval);
            }
        }

        private Thread SpawnWorker() {
            Thread workerThread = new Thread(() => {
                WorkJob job;
                int wqc = 0;
                while(Active || wqc > 0) {
                    lock (workQueue) {
                        if ((wqc = workQueue.Count) < 1)
                            return;

                        job = workQueue.Dequeue();
                    }
                    List<Exception> exes = new List<Exception>();
                    lock (job) {
                        if (job == null) {
                            return;
                        }

                        job.dequeued = DateTime.Now;
                        Fi.Tech.WriteLine($"[{Thread.CurrentThread.Name}] Job {this.QID}/{job.id} dequeued for execution after {(job.dequeued.Value - job.enqueued.Value).TotalMilliseconds}ms");

                        try {
                            job?.action?.Invoke();
                            job.status = WorkJobStatus.Finished;
                            job?.finished?.Invoke();
                            job.completed = DateTime.Now;
                            Fi.Tech.WriteLine($"[{Thread.CurrentThread.Name}] Job {this.QID}/{job.id} finished in {(job.completed.Value - job.dequeued.Value).TotalMilliseconds}ms");
                        }
                        catch (Exception x) {
                            job.completed = DateTime.Now;
                            Fi.Tech.WriteLine($"[{Thread.CurrentThread.Name}] Job {this.QID}/{job.id} failed in {(job.completed.Value - job.dequeued.Value).TotalMilliseconds}ms with message: {x.Message}");
                            job?.handling?.Invoke(x);
                        }
                        job.status = WorkJobStatus.Finished;
                        WorkDone++;
                    }
                    job = null;
                }
            });
            workerThread.Priority = DefaultWorkerPriority;
            workerThread.Name = $"FTWQ_{Name}_Worker_{workers.Count+1}";
            workerThread.Start();
            return workerThread;
        }

        public void Close() {
            closed = true;
        }

        public void Start() {
            if (Active || isRunning)
                return;
            Active = true;
            workers.RemoveAll(w => w.ThreadState != ThreadState.Running);
            while(workers.Count < parallelSize) {
                workers.Add(SpawnWorker());
            }
            _supervisor = new Thread(() => SupervisorJob());
            _supervisor.Name = $"FTWQ_{Name}_supervisor";
            _supervisor.IsBackground = true;
            _supervisor.Priority = ThreadPriority.BelowNormal;
            _supervisor.Start();
            isRunning = true;
        }

        public static void Live(Action<WorkQueuer> act, int parallelSize = -1) {
            if (parallelSize <= 0)
                parallelSize = Environment.ProcessorCount;
            var queuer = new WorkQueuer($"AnnonymousLiveQueuer", parallelSize);
            queuer.Start();
            act(queuer);
            queuer.Stop();
        }
        Logger logger = new Logger(new FileAccessor("Logs/BackgroundWork"));
        public void AccompanyJob(Action a, Action f = null, Action<Exception> e = null) {
            var wj = Enqueue(a, f, e);
            wj.Await();
        }

        private DateTime WentIdle = DateTime.UtcNow;

        public int TotalWork = 0;
        public int WorkDone = 0;

        public WorkJob Enqueue(Action a, Action finished = null, Action<Exception> exceptionHandler = null) {
            TotalWork++;
            var retv = new WorkJob(a, finished, exceptionHandler);
            Fi.Tech.WriteLine($"{this.Name}({this.QID}) Job: {this.QID}/{retv.id}");
            lock(workQueue) {
                workQueue.Enqueue(retv);
            }

            return retv;
        }

        public void Dispose() {
            Stop();
        }
    }
}
