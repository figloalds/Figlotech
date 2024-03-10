using Figlotech.BDados.Builders;
using Figlotech.Core;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public static class ILegacyLoadAllOptionsShimExtensions {

        public static List<T> LoadAll<T>(this IRdbmsDataAccessor self, string query, params object[] args) where T : IDataObject, new() {
            return self.LoadAll((IQueryBuilder)Qb.Fmt(query, args), (int?)null, (int?)null, (Expression<Func<T, object>>)null, OrderingType.Asc, (object)null);
        }
        public static List<T> LoadAll<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, string query, params object[] args) where T : IDataObject, new() {
            return self.LoadAll(transaction, (IQueryBuilder)Qb.Fmt(query, args), (int?)null, (int?)null, (Expression<Func<T, object>>)null, OrderingType.Asc, (object)null);
        }
        public static List<T> Query<T>(this IRdbmsDataAccessor self, string query, params object[] args) where T : IDataObject, new() {
            return self.Query<T>((IQueryBuilder)Qb.Fmt(query, args));
        }
        public static List<T> Query<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, string query, params object[] args) where T : IDataObject, new() {
            return self.Query<T>(transaction, (IQueryBuilder)Qb.Fmt(query, args));
        }
        public static DataTable Query(this IRdbmsDataAccessor self, string query, params object[] args) {
            return self.Query(Qb.Fmt(query, args));
        }
        public static DataTable Query(this IRdbmsDataAccessor self, BDadosTransaction transaction, string query, params object[] args) {
            return self.Query(transaction, Qb.Fmt(query, args));
        }
        public static int Execute(this IRdbmsDataAccessor self, string query, params object[] args) {
            return self.Execute((IQueryBuilder)Qb.Fmt(query, args));
        }
        public static int Execute(this IRdbmsDataAccessor self, BDadosTransaction transaction, string query, params object[] args) {
            return self.Execute(transaction, (IQueryBuilder)Qb.Fmt(query, args));
        }

        public static List<T> LoadAll<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return self.LoadAll<T>(
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static T LoadFirstOrDefault<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadFirstOrDefault<T>(
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> Fetch<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadAll<T>(
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static List<T> AggregateLoad<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, MemberInfo GroupingMember = null, bool Linear = false)
           where T : IDataObject, new() {
            return self.AggregateLoad<T>(
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
                    .GroupBy(GroupingMember)
                    .LinearIf(Linear)
            );
        }
    }

    public static class ILegacyRecordsetShimExtensions {
        public static RecordSet<T> LoadAll<T>(this RecordSet<T> self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return self.LoadAll(
                Core.Interfaces.LoadAll.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }
        public static RecordSet<T> LoadAllLinear<T>(this RecordSet<T> self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadAllLinear(
                Core.Interfaces.LoadAll.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> Fetch<T>(this RecordSet<T> self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return self.Fetch(
                Core.Interfaces.LoadAll.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> FetchLinear<T>(this RecordSet<T> self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.FetchLinear(
                Core.Interfaces.LoadAll.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }
    }

    public static class ILegacyRdbmsLoadAllOptionsShimExtensions {
        public static List<T> LoadAll<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return self.LoadAll<T>(
                transaction,
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }
        public static async Task<List<T>> LoadAllAsync<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return await Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
                    .Using(transaction)
                    .LoadAsync().ConfigureAwait(false);
        }

        public static async Task<List<T>> QueryAsync<T>(this IRdbmsDataAccessor self, IQueryBuilder query)
            where T : new() {
            return await self.AccessAsync(async tsn=> {
                return await self.QueryAsync<T>(tsn, query).ConfigureAwait(false);
            }, CancellationToken.None).ConfigureAwait(false);
        }

        public static async Task<List<T>> LoadAllAsync<T>(this IRdbmsDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return await self.AccessAsync(async transaction => {
                return await Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
                    .Using(transaction)
                    .LoadAsync().ConfigureAwait(false);
            }, CancellationToken.None).ConfigureAwait(false);
        }

        public static T LoadFirstOrDefault<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadFirstOrDefault<T>(
                transaction,
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> Fetch<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadAll<T>(
                transaction,
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static List<T> AggregateLoad<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, MemberInfo GroupingMember = null, bool Linear = false)
           where T : IDataObject, new() {
            return self.AggregateLoad<T>(
                transaction,
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
                    .GroupBy(GroupingMember)
                    .LinearIf(Linear)
            );
        }
        public static async Task<List<T>> AggregateLoadAsync<T>(this IRdbmsDataAccessor self, BDadosTransaction transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, MemberInfo GroupingMember = null, bool Linear = false)
           where T : IDataObject, new() {
            return await self.AggregateLoadAsync<T>(
                transaction,
                Core.Interfaces.LoadAll.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
                    .GroupBy(GroupingMember)
                    .LinearIf(Linear)
            ).ConfigureAwait(false);
        }
        public static async Task<List<T>> AggregateLoadAsync<T>(this IRdbmsDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, MemberInfo GroupingMember = null, bool Linear = false)
           where T : IDataObject, new() {
            return await self.AccessAsync(async transaction=> {
                return await self.AggregateLoadAsync<T>(
                    transaction,
                    Core.Interfaces.LoadAll.Where<T>(conditions)
                        .Skip(skip)
                        .Limit(limit)
                        .OrderBy(orderingMember, ordering)
                        .GroupBy(GroupingMember)
                        .LinearIf(Linear)
                ).ConfigureAwait(false);
            }, CancellationToken.None).ConfigureAwait(false);
        }
    }
}
