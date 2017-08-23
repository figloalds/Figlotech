using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Figlotech.BDados.Builders;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Data;
using System.Diagnostics;

namespace Figlotech.BDados.DataAccessAbstractions {

    public class MDAUtask {
        public Type Type;
        public long LastSyncUp = 0;
        public long LastSyncDown = 0;
    }

    public class MixedDataAccessor : IDataAccessor {
        protected IDataAccessor MainAccessor;
        protected IDataAccessor PersistenceAccessor;

        bool BypassMode = true;

        private WorkQueuer Queuer = new WorkQueuer("MixedDataAccessor", Environment.ProcessorCount+1);

        ILogger _logger = new Logger();

        public T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = default(int?), int? limit = 200) where T : IDataObject, new() {
            return LoadAll<T>(condicoes, page, limit).FirstOrDefault();
        }
        List<MDAUtask> typesynctimes = new List<MDAUtask>();

        public MixedDataAccessor(IDataAccessor main, IDataAccessor persistence) {
            MainAccessor = main;
            PersistenceAccessor = persistence;

        }

        public Type[] _workingTypes;
        public Type[] WorkingTypes {
            get { return _workingTypes; }
            set {
                _workingTypes = value.Where(t => t.GetInterfaces().Contains(typeof(IDataObject))).ToArray();
            }
        }

        public void SetBypass(bool value) {
            BypassMode = value;
        }

        public void SyncDown(Type t) {
            var mdauts = new MDAUtask {
                Type = t
            };
            if(!typesynctimes.Any(s=>s.Type == t)) {
                typesynctimes.Add(mdauts);
            }
            mdauts = typesynctimes.FirstOrDefault(s => s.Type == t);
            var moment = DateTime.UtcNow.Ticks;
            var rs = FTH.LoadAllOfByUpdateTime(t, PersistenceAccessor, mdauts.LastSyncDown, 1, null);
            mdauts.LastSyncDown = moment;
            FTH.SetAccessorOfRecordSet(rs, MainAccessor);
            FTH.SaveRecordSet(rs);
        }

        public void SyncUp(Type t) {
            var mdauts = new MDAUtask {
                Type = t
            };
            if (!typesynctimes.Any(s => s.Type == t)) {
                typesynctimes.Add(mdauts);
            }
            mdauts = typesynctimes.FirstOrDefault(s => s.Type == t);
            var moment = DateTime.UtcNow.Ticks;
            var rs = FTH.LoadAllOfByUpdateTime(t, MainAccessor, mdauts.LastSyncDown, 1, null);
            mdauts.LastSyncUp = moment;
            FTH.SetAccessorOfRecordSet(rs, PersistenceAccessor);
            FTH.SaveRecordSet(rs);
        }

        public void Init(Type[] types, Action finished = null) {
            BypassMode = true;
            WorkingTypes = types;
            bool release = false;
            Queuer.Start();
            Queuer.Enqueue((pg) => {
                while (Queuer.Run) {
                    List<WorkJob> jobs = new List<WorkJob>();
                    foreach (var ty in WorkingTypes) {
                        jobs.Add(
                            Queuer.Enqueue((p) => {
                                SyncDown(ty);
                            }));
                        jobs.Add(
                            Queuer.Enqueue((p) => {
                                SyncUp(ty);
                            }));
                    }
                    foreach (var a in jobs) {
                        a.Accompany();
                    }
                    if (BypassMode) {
                        BypassMode = false;
                        release = true;
                    }
                    Thread.Sleep(10000);
                }
            });
            while (!release) {
                Thread.Sleep(10);
            }
            finished?.Invoke();

        }

        public ILogger Logger {
            get {
                return _logger;
            }

            set {
                _logger = value;
            }
        }


        private IDataAccessor SecondaryAccessor {
            get {
                if (!BypassMode)
                    return PersistenceAccessor;
                else
                    return MainAccessor;
            }
        }
        private IDataAccessor UsableAccessor {
            get {
                if (BypassMode)
                    return PersistenceAccessor;
                else
                    return MainAccessor;
            }
        }
        
        public RecordSet<T> AggregateLoad<T>(Expression<Func<T, bool>> cnd = null, int? limit = default(int?), int? page = default(int?), int PageSize = 200, MemberInfo OrderingMember = null, OrderingType Ordering = OrderingType.Asc, bool Linear = false) where T : IDataObject, new() {
            return UsableAccessor.AggregateLoad<T>(cnd, limit, page, PageSize, OrderingMember, Ordering, Linear);
        }

        public bool Delete(IDataObject obj) {
            return UsableAccessor.Delete(obj);
        }

        public bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new() {
            return UsableAccessor.Delete<T>(condition);
        }

        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new() {
            return UsableAccessor.DeleteWhereRidNotIn<T>(cnd, rids);
        }

        public T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new() {
            return UsableAccessor.ForceExist<T>(Default, cnd);
        }

        public RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> condicoes, int? page = default(int?), int? limit = 200) where T : IDataObject, new() {
            return UsableAccessor.LoadAll<T>(condicoes, page??1, limit);
        }

        public T LoadById<T>(long Id) where T : IDataObject, new() {
            return UsableAccessor.LoadById<T>(Id);
        }

        public T LoadByRid<T>(String RID) where T : IDataObject, new() {
            return UsableAccessor.LoadByRid<T>(RID);
        }

        public bool SaveItem(IDataObject objeto, Action funcaoPosSalvar = null) {
            var retv = UsableAccessor.SaveItem(objeto, funcaoPosSalvar);
            if(UsableAccessor != PersistenceAccessor) {
                Queuer.Enqueue((p) => {
                    PersistenceAccessor.SaveItem(objeto, null);
                });
            }
            return retv;
        }
        
        public bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new() {
            Queuer.Enqueue((p) => {
                SecondaryAccessor.SaveRecordSet(rs);
            });
            return UsableAccessor.SaveRecordSet<T>(rs);
        }
    }
}
