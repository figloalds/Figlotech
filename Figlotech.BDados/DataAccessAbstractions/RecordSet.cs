/**
* Figlotech::Database::Entity::BDConjunto
* Extensão para List<TSource>, BDConjunto<TSource> where T: BDTabela, new()
* Permite armazenar vários objetos de um mesmo tipo e salvar/excluir em massa.
* 
*@Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using Figlotech.Core.BusinessModel;
using System;
using System.Collections.Generic;
using System.Linq;
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
        public MemberInfo GroupingMember = null;
        private Func<T, object> orderingExpression = null;
        public RecordSet<T> OrderBy(Expression<Func<T, object>> fn, OrderingType orderingType) {
            try {
                orderingExpression = fn.Compile();
                OrderingMember = FindMember(fn.Body);
                Ordering = orderingType;
            } catch (Exception) { }

            return this;
        }

        public RecordSet<T> SetGroupingMember(MemberInfo mbi) {
            GroupingMember = mbi;
            return this;
        }

        public RecordSet<T> GroupResultsBy(Expression<Func<T, object>> fn) {
            try {
                GroupingMember = FindMember(fn.Body);
            } catch (Exception) { }

            return this;
        }

        int? LimitResults = BDadosConfiguration.DefaultResultsLimit;

        public RecordSet<T> Limit(int? limits) {
            LimitResults = limits;

            return this;
        }

        public RecordSet<T> LoadAll(Expression<Func<T, bool>> cnd = null, int? page = null) {
            AddRange(Fetch(cnd, page));
            return this;
        }

        public RecordSet<T> LoadAllLinear(Expression<Func<T, bool>> cnd = null, int? page = null) {
            AddRange(FetchLinear(cnd, page));
            return this;
        }

        public IEnumerable<T> Fetch(Expression<Func<T,bool>> cnd = null, int? page = null, bool Linear = false) {
            Clear();

            var agl = DataAccessor.AggregateLoad<T>(cnd, OrderingMember, Ordering, LimitResults, page, PageSize,  GroupingMember, OrderingMember, Ordering, Linear);
            agl = agl.ToList();
            if (OrderingMember != null) {
                if(Ordering == OrderingType.Desc) {
                    agl = agl.OrderByDescending(orderingExpression);
                } else {
                    agl = agl.OrderBy(orderingExpression);
                }
            }
            var enumerator = agl.GetEnumerator();
            while(enumerator.MoveNext()) {
                var transport = enumerator.Current;
                yield return transport;
            }
            
            LimitResults = null;
            orderingExpression = null;
            OrderingMember = null;
            GroupingMember = null;
        }

        public IEnumerable<T> FetchLinear(Expression<Func<T, bool>> cnd = null, int? page = null) {
            LinearLoad = true;
            var retv = Fetch(cnd, page, true);
            LinearLoad = false;
            return retv;
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
