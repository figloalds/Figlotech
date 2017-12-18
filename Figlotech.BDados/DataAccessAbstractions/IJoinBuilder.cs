using System;
using System.Data;
using Figlotech.Core.Interfaces;
using Figlotech.BDados.Helpers;
using System.Collections.Generic;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IJoinBuilder {
        IQueryBuilder GenerateQuery(IQueryGenerator generator, IQueryBuilder conditions, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, int p = 1, int limit = 200, IQueryBuilder conditionsRoot = null);
        DataTable GenerateDataTable(ConnectionInfo transaction, IQueryGenerator generator, IQueryBuilder conditions, int? p = 1, int? limit = 200, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, IQueryBuilder conditionsRoot = null);
        List<T> BuildObject<T>(ConnectionInfo transaction, Action<BuildParametersHelper> fn, IQueryBuilder conditions, int? skip = 1, int? limit = 200, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, IQueryBuilder conditionsRoot = null) where T: IDataObject, new();
    }
}
