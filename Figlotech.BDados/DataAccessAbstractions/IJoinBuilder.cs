using System;
using System.Data;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.Helpers;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IJoinBuilder {
        IQueryBuilder GenerateQuery(IQueryGenerator generator, IQueryBuilder conditions, int p = 1, int limit = 200, IQueryBuilder conditionsRoot = null);
        DataTable GenerateDataTable(IQueryGenerator generator, IQueryBuilder conditions, int? p = 1, int? limit = 200, IQueryBuilder conditionsRoot = null);
        RecordSet<T> BuildObject<T>(Action<BuildParametersHelper> fn, IQueryBuilder conditions, int? p = 1, int? limit = 200, IQueryBuilder conditionsRoot = null) where T: IDataObject, new();
    }
}
