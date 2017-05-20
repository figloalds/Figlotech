using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity {

    public enum WorkJobStatus {
        Queued,
        Running,
        Finished
    }

    public class WorkSchedule {
        public WorkJob Job;
        public bool Repeat = false;
        public TimeSpan Interval;
        public DateTime Start;
        public WorkQueuer Parent;

        public WorkSchedule(WorkQueuer parent, Action act, Action finished, Action<Exception> handle, DateTime start, bool repeat = false, TimeSpan interval = default(TimeSpan)) {
            Job = new WorkJob(parent, act, finished, handle);
            Parent = parent;
            Start = start;
            Interval = interval;
            Repeat = repeat;
        }

    }

    public class WorkJob {
        public Action action;
        public Action finished;
        public Action<Exception> handling;
        public WorkQueuer queuer;
        public WorkJobStatus status;

#if DEBUG
        public StackFrame[] ContextStack;
#endif

        public WorkJob(WorkQueuer parent, Action method, Action actionWhenFinished, Action<Exception> errorHandling) {
            queuer = parent;
            action = method;
            finished = actionWhenFinished;
            handling = errorHandling;
#if DEBUG
            StackTrace stackTrace = new StackTrace();   // get call stack
            ContextStack = stackTrace.GetFrames();      // get method calls (frames)
#endif
        }

        public void Accompany() {
            WorkQueuer.AccompanyJob(this);
        }
    }
    public class WorkQueuer : IDisposable {
        public static int qid_increment = 0;
        private int QID = ++qid_increment;
        String Name;
        Queue<WorkJob> work = new Queue<WorkJob>();
        List<WorkSchedule> schedules = new List<WorkSchedule>();
        Queue<Thread> workers = new Queue<Thread>();
        Thread SchedulesThread;
        private bool run = false;
        private bool isRunning = false;
        public static int DefaultSleepInterval = 10;

        public bool Run { get { return run; } }
        public bool IsRunning { get { return isRunning; } }
        private bool isPaused = false;

        int parallelSize = 1;

        public static List<WorkQueuer> WorldQueuers = new List<WorkQueuer>();

        public WorkQueuer(String name, int maxThreads = -1, bool init_started = false) {
            if(maxThreads <= 0) {
                maxThreads = Environment.ProcessorCount-1;
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
                a.Stop();
            });
            //WorldQueuers.Clear();
        }

        public void Pause() {

        }

        public void Stop() {
            run = false;
            while (workers.Count > 0) {
                try {
                    workers.Dequeue().Join();
                } catch (Exception) { }
            }
            try {
                SchedulesThread.Join();
            } catch(Exception x) { }
            //while(work.Count > 0) {
            //    Console.WriteLine($"bug {work.Count}");
            //}
            isRunning = false;
        }

        private void WorkersJob() {
            int i = 1;
            int w = 100;
            while (true) {
                //if (!run) {
                //    return;
                //}
                List<Exception> exes = new List<Exception>();
                WorkJob job = null;
                lock (work) {
                    while(run && work.Count < 1) {
                        Thread.Sleep(DefaultSleepInterval);
                    }
                    try {
                        job = work.Dequeue();
                        job.status = WorkJobStatus.Running;
                    } catch (Exception x) {
                    }
                    //Console.WriteLine(x.Message);
                }
                if (job == null) {
                    if (!run && work.Count == 0) {
                        return;
                    }

                    w = w < (100 * parallelSize) ? w + 10 : w;
                    Thread.Sleep(w);
                    continue;
                }
                w = DefaultSleepInterval;

                try {
                    job?.action?.Invoke();
                    job.status = WorkJobStatus.Finished;
                    job?.finished?.Invoke();
                } catch (Exception x) {
                    Console.WriteLine($"WQ({QID}): {x.Message}");
                    job?.handling?.Invoke(x);
                }
                job.status = WorkJobStatus.Finished;

            }
        }

        public void Start() {
            if (run || isRunning)
                return;
            run = true;
            while (workers.Count < parallelSize) {
                var th = new Thread(WorkersJob);
                th.Name = $"QU{QID}({Name})_WT{workers.Count + 1}";
                workers.Enqueue(th);
                th.Priority = ThreadPriority.Lowest;
                th.Start();
            }
            SchedulesThread = new Thread(() => {
                while(Run) {
                    lock (schedules) {
                        for (int x = 0; x < schedules.Count; x++) {
                            if (schedules[x].Start < DateTime.UtcNow) {
                                var sched = schedules[x];
                                schedules.RemoveAt(x);
                                this.Enqueue(sched.Job.action, () => {
                                    if(sched.Repeat) {
                                        sched.Start += sched.Interval;
                                        schedules.Add(sched);
                                    }
                                    sched.Job.finished?.Invoke();
                                });
                                break;
                            }
                        }
                    }
                    Thread.Sleep(1000);
                }
            });
            SchedulesThread.Name = $"WQ{QID}_{Name}_Scheduler";
            SchedulesThread.Start();
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

        public void AccompanyJob(Action a, Action f = null, Action<Exception> e = null) {
            var wj = Enqueue(a, f, e);
            wj.Accompany();
        }

        public static void AccompanyJob(WorkJob wj) {
            DateTime dt = DateTime.UtcNow;
            while (wj.status != WorkJobStatus.Finished || wj.queuer.work.Contains(wj)) {
                if (!wj.queuer.isRunning && wj.status != WorkJobStatus.Finished)
                    throw new Exception($"Queuer is no longer running but the work job being accompanied is still {wj.status.ToString()}");
                if (wj.status == WorkJobStatus.Queued)
                    Thread.Sleep(DefaultSleepInterval * 5);
                else
                    Thread.Sleep(DefaultSleepInterval);
                //if (DateTime.UtcNow.Subtract(dt).TotalMilliseconds > 10000) {
                //    throw new Exception($"Job was being accompanied for longer than the timeout, its status is still {wj.status}");
                //}
            }
        }

        public WorkSchedule OneTimeSched(DateTime dt, Action a, Action finished = null, Action<Exception> handler = null) {
            lock(schedules) {
                var sched = new WorkSchedule(this, a, finished, handler, dt);
                schedules.Add(sched);

                return sched;
            }
        }
        public WorkSchedule RecurringSched(DateTime dt, TimeSpan interval, Action a, Action finished = null, Action<Exception> handler = null) {
            lock(schedules) {
                var sched = new WorkSchedule(this, a, finished, handler, dt, true, interval);
                schedules.Add(sched);

                return sched;
            }
        }

        public WorkJob Enqueue(Action a, Action finished = null, Action<Exception> exceptionHandler = null) {
            var retv = new Entity.WorkJob(this, a, finished, exceptionHandler);
            work.Enqueue(retv);
            retv.status = WorkJobStatus.Queued;

            return retv;
        }

        public void Dispose() {
            Stop();
        }
    }
}
