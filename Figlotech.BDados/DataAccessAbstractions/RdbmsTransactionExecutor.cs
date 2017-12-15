using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Figlotech.BDados.Builders;
using Figlotech.Core.Interfaces;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public class RdbmsTransactionExecutor : IDataAccessor
    {
        IRdbmsDataAccessor parent;
        IDbTransaction transaction;
        internal RdbmsTransactionExecutor(IRdbmsDataAccessor parent, IDbTransaction transaction) {
            this.transaction = transaction;
            this.parent = parent;
        }

        public ILogger Logger { get => parent.Logger; set => parent.Logger = value; }
        public Type[] WorkingTypes { get => parent.WorkingTypes; set => parent.WorkingTypes = value; }

        public IEnumerable<T> AggregateLoad<T>(Expression<Func<T, bool>> cnd = null, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, int? limit = null, int? page = null, int PageSize = 200, MemberInfo GroupingMember = null, MemberInfo OrderingMember = null, OrderingType Ordering = OrderingType.Asc, bool Linear = false) where T : IDataObject, new() {
            return parent.AggregateLoad(transaction, cnd, orderingMember, otype, limit, page, PageSize, GroupingMember, OrderingMember, Ordering, Linear);
        }

        public bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new() {
            return parent.Delete(transaction, condition);
        }

        public bool Delete(IDataObject obj) {
            return parent.Delete(transaction, obj);
        }

        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new() {
            return parent.DeleteWhereRidNotIn(transaction, cnd, rids);
        }

        public IEnumerable<T> Fetch<T>(Expression<Func<T, bool>> condicoes, int? page = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return parent.Fetch(transaction, condicoes, page, limit, orderingMember, ordering);
        }

        public T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new() {
            //return parent.ForceExist(transaction, Default, cnd);
            throw new NotImplementedException();
        }

        public RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> condicoes, int? page = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return parent.LoadAll(transaction, condicoes, page, limit, orderingMember, ordering);
        }

        public T LoadById<T>(long Id) where T : IDataObject, new() {
            return parent.LoadById<T>(transaction, Id);
        }

        public T LoadByRid<T>(string RID) where T : IDataObject, new() {
            return parent.LoadByRid<T>(transaction, RID);
        }

        public T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return parent.LoadFirstOrDefault(transaction, condicoes, page, limit, orderingMember, ordering);
        }

        public bool SaveItem(IDataObject objeto) {
            return parent.SaveItem(transaction, objeto);
        }

        public bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new() {
            return parent.SaveRecordSet(transaction, rs);
        }
    }
}
