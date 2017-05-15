using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Figlotech.BDados.Entity {
    public class MixedRdbmsDataAccessor : MixedDataAccessor, IRdbmsDataAccessor {
        public string SchemaName {
            get {
                return RdbmsAccessor.SchemaName;
            }
        }

        public MixedRdbmsDataAccessor(IDataAccessor main, IDataAccessor secondary) : base(main,secondary) {

        }

        private IRdbmsDataAccessor RdbmsAccessor {
            get {
                if (MainAccessor is IRdbmsDataAccessor)
                    return MainAccessor as IRdbmsDataAccessor;
                if (PersistenceAccessor is IRdbmsDataAccessor)
                    return PersistenceAccessor as IRdbmsDataAccessor;
                return null;
            }
        }

        public object Access(Action<IRdbmsDataAccessor> funcaoAcessar, Action<Exception> trataErros = null) {
            return RdbmsAccessor.Access(funcaoAcessar, trataErros);
        }

        public bool CheckStructure(IEnumerable<Type> types, bool resetKeys = true) {
            return RdbmsAccessor.CheckStructure(types, resetKeys);
        }

        public int Execute(IQueryBuilder Query) {
            return RdbmsAccessor.Execute(Query);
        }

        public int Execute(string Query, params object[] args) {
            return RdbmsAccessor.Execute(Query, args);
        }

        public T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new() {
            return RdbmsAccessor.ForceExist<T>(Default, qb);
        }

        public T ForceExist<T>(Func<T> Default, string query, params object[] args) where T : IDataObject, new() {
            return RdbmsAccessor.ForceExist<T>(Default, query, args);
        }

        public IQueryBuilder GetPreferredQueryBuilder() {
            return RdbmsAccessor.GetPreferredQueryBuilder();
        }

        public IQueryGenerator GetQueryGenerator() {
            return RdbmsAccessor.GetQueryGenerator();
        }

        public RecordSet<T> LoadAll<T>(IQueryBuilder condicoes) where T : IDataObject, new() {
            return RdbmsAccessor.LoadAll<T>(condicoes);
        }

        public RecordSet<T> LoadAll<T>(string where = "TRUE", params object[] args) where T : IDataObject, new() {
            return RdbmsAccessor.LoadAll<T>(where, args);
        }

        public IJoinBuilder MakeJoin(Action<JoinDefinition> fn) {
            return RdbmsAccessor.MakeJoin(fn);
        }

        public DataTable Query(IQueryBuilder Query) {
            return RdbmsAccessor.Query(Query);
        }

        public DataTable Query(string Query, params object[] args) {
            return RdbmsAccessor.Query(Query, args);
        }

        public List<T> Query<T>(List<T> input, IQueryBuilder Query) {
            return RdbmsAccessor.Query<T>(input, Query);
        }

        public List<T> Query<T>(List<T> input, string Query, params object[] args) {
            return RdbmsAccessor.Query<T>(input, Query, args);
        }
    }
}
