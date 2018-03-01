using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public class WorkSchedule {
        public WorkJob Job;
        public bool Repeat = false;
        public TimeSpan Interval;
        public DateTime Start;
        public WorkScheduler Parent;

        public WorkSchedule(WorkScheduler parent, Action act, Action<Exception> handle, Action finished, DateTime start, bool repeat = false, TimeSpan interval = default(TimeSpan)) {
            Job = new WorkJob(() => { act(); }, handle, ()=> { finished(); });
            Parent = parent;
            Start = start;
            Interval = interval;
            Repeat = repeat;
        }

    }


    public class WorkScheduler : IContinuousExecutor {
        List<WorkSchedule> schedules = new List<WorkSchedule>();
        Thread SchedulesThread;
        private bool isRunning;

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
        static int QIDGen = 1;
        public string Name { get; }
        public int QID { get; private set; } = ++QIDGen;

        public WorkScheduler(String name, bool init_started = false) {
            Name = name;

            if (init_started)
                Start();
        }

        public WorkSchedule OneTimeSched(DateTime dt, Action a, Action finished = null, Action<Exception> handler = null) {
            lock (schedules) {
                var sched = new WorkSchedule(this, a,handler, finished, dt);
                schedules.Add(sched);

                return sched;
            }
        }
        public WorkSchedule RecurringSched(DateTime dt, TimeSpan interval, Action a, Action finished = null, Action<Exception> handler = null) {
            lock (schedules) {
                var sched = new WorkSchedule(this, a, handler, finished, dt, true, interval);
                schedules.Add(sched);

                return sched;
            }
        }

        public void Start() {
            if (Active || isRunning)
                return;
            Active = true;
        }

        public void Stop(bool wait = true) {
            Active = false;
            if (wait) {
                try {
                    SchedulesThread.Join();
                }
                catch (Exception) { }
            }
            isRunning = false;
        }
    }
}
