using Figlotech.BDados.Builders;
using Figlotech.BDados.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {
    public delegate void BuildHelper(IQueryBuildHelper qh);
    public interface IQueryGenerator {
        IQueryBuilder GenerateInsertQuery(IDataObject<T>tabelaInput);
        IQueryBuilder GenerateUpdateQuery(IDataObject<T>tabelaInput);
        IQueryBuilder GenerateSaveQuery(IDataObject<T>tabelaInput);
        IQueryBuilder GenerateSelectAll<T>() where T : IDataObject<T>, new();
        IQueryBuilder GenerateSelect<T>(IQueryBuilder condicoes) where T : IDataObject<T>, new();
        IQueryBuilder GenerateJoinQuery(JoinDefinition juncaoInput, IQueryBuilder condicoes, int? p = 1, int? limit = 100, IQueryBuilder condicoesRoot = null);
        IQueryBuilder GenerateMultiInsert<T>(RecordSet<T> conjuntoInput) where T : IDataObject<T>, new();
        IQueryBuilder GenerateMultiUpdate<T>(RecordSet<T> inputRecordset) where T : IDataObject<T>, new();
        IQueryBuilder GenerateCallProcedure(string name, object[] args);
        IQueryBuilder GetCreationCommand<T>() where T : IDataObject<T>
    }
}
