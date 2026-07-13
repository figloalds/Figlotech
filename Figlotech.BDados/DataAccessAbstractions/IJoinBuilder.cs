using Figlotech.Core.Interfaces;
using Figlotech.Data;
using System;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IJoinBuilder {
        IQueryBuilder GenerateQuery(IQueryGenerator generator, IQueryBuilder conditions, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, int? p = null, int? limit = null, IQueryBuilder conditionsRoot = null);
        [Obsolete("Legacy mutable construction access is not safe for execution. Use DefinitiveJoinPlanExtensions.BuildPlan or the frozen GenerateQuery overloads instead.")]
        JoinDefinition GetJoin();
    }
}
