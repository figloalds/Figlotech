using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public static class ILegacyLoadAllOptionsShimExtensions {
        public static List<T> LoadAll<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return self.LoadAll<T>(
                FetchData.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static T LoadFirstOrDefault<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadFirstOrDefault<T>(
                FetchData.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> Fetch<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadAll<T>(
                FetchData.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static List<T> AggregateLoad<T>(this IDataAccessor self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, MemberInfo GroupingMember = null, bool Linear = false)
           where T : IDataObject, new() {
            return self.AggregateLoad<T>(
                FetchData.Where<T>(conditions)
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
                FetchData.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }
        public static RecordSet<T> LoadAllLinear<T>(this RecordSet<T> self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadAllLinear(
                FetchData.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> Fetch<T>(this RecordSet<T> self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return self.Fetch(
                FetchData.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> FetchLinear<T>(this RecordSet<T> self, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.FetchLinear(
                FetchData.Where(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }
    }

    public static class ILegacyRdbmsLoadAllOptionsShimExtensions {
        public static List<T> LoadAll<T>(this IRdbmsDataAccessor self, ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
            where T : IDataObject, new() {
            return self.LoadAll<T>(
                transaction,
                FetchData.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static T LoadFirstOrDefault<T>(this IRdbmsDataAccessor self, ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadFirstOrDefault<T>(
                transaction,
                FetchData.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static IEnumerable<T> Fetch<T>(this IRdbmsDataAccessor self, ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc)
           where T : IDataObject, new() {
            return self.LoadAll<T>(
                transaction,
                FetchData.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
            );
        }

        public static List<T> AggregateLoad<T>(this IRdbmsDataAccessor self, ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, MemberInfo GroupingMember = null, bool Linear = false)
           where T : IDataObject, new() {
            return self.AggregateLoad<T>(
                transaction,
                FetchData.Where<T>(conditions)
                    .Skip(skip)
                    .Limit(limit)
                    .OrderBy(orderingMember, ordering)
                    .GroupBy(GroupingMember)
                    .LinearIf(Linear)
            );
        }
    }
}
