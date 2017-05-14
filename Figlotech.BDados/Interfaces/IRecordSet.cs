using Figlotech.BDados.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {
    public enum OrderingType {
        Asc,
        Desc
    }
    public interface IRecordSet<T> : ISaveable where T: IDataObject<T>, new() {
        //void LoadAll(IQueryBuilder conditions = null);
        IRecordSet<T> LoadAll(Expression<Func<T,bool>> conditions = null, int? page = null);
        IRecordSet<T> OrderBy(Expression<Func<T, object>> fn, OrderingType orderingType);
        String ListRids();
        
    }
}
