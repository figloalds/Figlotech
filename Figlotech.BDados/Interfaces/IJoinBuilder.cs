using Figlotech.BDados.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Figlotech.BDados.Entity;

namespace Figlotech.BDados.Interfaces {
    public interface IJoinBuilder {
        IQueryBuilder GenerateQuery(IQueryGenerator generator, IQueryBuilder conditions, int p = 1, int limit = 200, IQueryBuilder conditionsRoot = null);
        DataTable GenerateDataTable(IQueryGenerator generator, IQueryBuilder conditions, int? p = 1, int? limit = 200, IQueryBuilder conditionsRoot = null);
        RecordSet<T> BuildObject<T>(Action<BuildParametersHelper> fn, IQueryBuilder conditions, int? p = 1, int? limit = 200, IQueryBuilder conditionsRoot = null) where T: IDataObject<T>new();
    }
}
