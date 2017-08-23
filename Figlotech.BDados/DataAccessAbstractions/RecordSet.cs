/**
* Figlotech::Database::Entity::BDConjunto
* Extensão para List<TSource>, BDConjunto<TSource> where T: BDTabela, new()
* Permite armazenar vários objetos de um mesmo tipo e salvar/excluir em massa.
* 
*@Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public enum OrderingType {
        Asc,
        Desc
    }
    public class RecordSet<T> : List<T>, IEnumerable<T>
        where T : IDataObject, new() {

        internal IDataAccessor dataAccessor;

        public IDataAccessor DataAccessor { get; set; }

        public Type __BDadosTipoTabela {
            get {
                return typeof(T);
            }
        }

        public static int DefaultPageSize = 200;
        public int PageSize { get; set; } = DefaultPageSize;
        public bool LinearLoad = false;

        public RecordSet() { }
        public RecordSet(IDataAccessor dataAccessor) {
            this.DataAccessor = dataAccessor;
        }

        public T FirstOrDefault() {
            return this.Count > 0 ? this[0] : default(T);
        }
        
        public String CustomListing(Func<T, String> fn) {
            StringBuilder retv = new StringBuilder();
            for (int i = 0; i < this.Count; i++) {
                retv.Append(
                    String.Format(
                        $"'{fn((this[i]))}'",
                        this[i].RID
                    )
                );
                if (i < this.Count - 1)
                    retv.Append(",");
            }
            return retv.ToString();
        }
        
        private MemberInfo FindMember(Expression x) {
            if(x is UnaryExpression) {
                return FindMember((x as UnaryExpression).Operand);
            }
            if(x is MemberExpression) {
                return (x as MemberExpression).Member;
            }

            return null;
        }

        public OrderingType Ordering = OrderingType.Asc;
        public MemberInfo OrderingMember = null;
        public RecordSet<T> OrderBy(Expression<Func<T, object>> fn, OrderingType orderingType) {
            try {
                OrderingMember = FindMember(fn.Body);
                Ordering = orderingType;
            } catch (Exception) { }

            return this;
        }

        int? LimitResults = BDadosConfiguration.DefaultResultsLimit;

        public RecordSet<T> Limit(int? limits) {
            LimitResults = limits;

            return this;
        }

        public RecordSet<T> LoadAll(Expression<Func<T,bool>> cnd = null, int? page = null) {
            Clear();
            var transport = DataAccessor.AggregateLoad<T>(cnd, LimitResults, page, PageSize, OrderingMember, Ordering, LinearLoad);
            foreach(var i in transport) {
                if(i is IBusinessObject bo) {
                    bo.OnAfterLoad();
                }
                this.Add(i);
            }
            transport = null;

            LimitResults = null;
            OrderingMember = null;

            return this;
        }

        public RecordSet<T> LoadAllLinear(Expression<Func<T, bool>> cnd = null, int? page = null) {
            LinearLoad = true;
            LoadAll(cnd, page);
            LinearLoad = false;
            return this;
        }

        public bool Load(Action fn = null) {
            if (dataAccessor == null) {
                return false;
            }
            LoadAll();
            return true;
        }

        public bool Save(Action fn = null) {
            return DataAccessor?.SaveRecordSet(this) ?? false;
        }

    }
}
