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
        public WorkJobStatus status;
        public DateTime? enqueued = DateTime.Now;
        public DateTime? dequeued;
        public DateTime? completed;
    }

    public class WorkJob<T> : WorkJob {
        public int id = ++idGen;
        private static int idGen = 0;
        public Func<T> action;
        public Action<T> finished;
        public Action<Exception> handling;

        internal Task<T> AssociatedTask { get; set; }

        public T ReturnValue { get; set; }

        public void Await() {
            while (completed == null) {

                Thread.Sleep(100);
            }
        }

        public async Task<T> Conclusion() {
            if (AssociatedTask != null && AssociatedTask.Status != TaskStatus.RanToCompletion)
                return await AssociatedTask;
            return default(T);
        }

        //#if DEBUG
        //        public StackFrame[] ContextStack;
        //#endif

        public WorkJob(Func<T> method, Action<Exception> errorHandling, Action<T> actionWhenFinished) {
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
        Queue<WorkJob> workQueue = new Queue<WorkJob>();
        List<WorkJob> holdJobs = new List<WorkJob>();
        List<Task> tasks = new List<Task>();

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

        int parallelSize = 1;

        ThreadPriority _defaultWorkerPriority = ThreadPriority.Normal;
        public ThreadPriority DefaultWorkerPriority {
            get {
                return _defaultWorkerPriority;
            }
            set {
                _defaultWorkerPriority = value;
            }
        }

        public WorkQueuer(String name, int maxThreads = -1, bool init_started = true) {
            if (maxThreads <= 0) {
                maxThreads = Environment.ProcessorCount - 1;
            }
            maxThreads = Math.Max(1, maxThreads);
            parallelSize = maxThreads;
            Name = name;
            if (init_started)
                Start();
        }

        public void Pause() {

        }

        public async Task Stop() {
            Active = false;
            while (tasks.Count > 0) {
                await tasks[tasks.Count - 1];
            }
            tasks.Clear();
            isRunning = false;
        }

        bool closed = false;

        public int ExtraWorkerTimeout { get; set; } = 7000;
        public int MainWorkerTimeout { get; set; } = 60000;
        public int MinWorkers { get; set; } = 0;

        public void Close() {
            closed = true;
        }

        public void Start() {
            if (Active || isRunning)
                return;
            Active = true;
            tasks.RemoveAll(w => w.IsCompleted);

            isRunning = true;
        }

        public static async Task Live(Action<WorkQueuer> act, int parallelSize = -1) {
            if (parallelSize <= 0)
                parallelSize = Environment.ProcessorCount;
            var queuer = new WorkQueuer($"AnnonymousLiveQueuer", parallelSize);
            queuer.Start();
            act(queuer);
            await queuer.Stop();
        }

        public async Task<T> AccompanyJob<T>(Func<T> a, Action<Exception> exceptionHandler = null, Action<T> finished = null) {
            var wj = Enqueue(a, exceptionHandler, finished);
            return await wj.Conclusion();
        }

        public int TotalWork = 0;
        public int WorkDone = 0;

        public WorkJob<int> Enqueue(Action action, Action<Exception> exceptionHandler = null, Action finished = null)
            => Enqueue(() => { action.Invoke(); return 0; }, (a) => exceptionHandler(a), (a) => finished.Invoke() );

        public WorkJob<T> Enqueue<T>(Func<T> a, Action<Exception> exceptionHandler = null, Action<T> finished = null) {
            TotalWork++;
            var retv = new WorkJob<T>(a, exceptionHandler, finished);
            Fi.Tech.WriteLine($"{this.Name}({this.QID}) Job: {this.QID}/{retv.id}");
            lock (workQueue) {
                workQueue.Enqueue(retv);
            }

            retv.AssociatedTask = Task.Run<T>(() => {
                try {
                    retv.ReturnValue = a.Invoke();
                } catch (Exception x) {
                    exceptionHandler?.Invoke(x);
                } finally {
                    finished?.Invoke(retv.ReturnValue);
                }
                return retv.ReturnValue;
            });

            return retv;
        }

        public void Dispose() {
            Stop().Wait();
        }
    }
}
