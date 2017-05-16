
using Figlotech.BDados.Attributes;
using Figlotech.BDados.Builders;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.Requirements;
using Figlotech.Core;
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
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity {
    public class RecordSet<T> : List<T>, IEnumerable<T>, IRequiresDataAccessor
        where T : IDataObject, new() {

        internal IDataAccessor dataAccessor;

        public IDataAccessor DataAccessor {
            get {
                return dataAccessor;
            }
            set {
                if(dataAccessor != value) {
                    foreach (var a in this) {
                        a.DataAccessor = dataAccessor;
                        a.Id = 0;
                    }
                    dataAccessor = value;
                }
            }
        }

        public Type __BDadosTipoTabela {
            get {
                return typeof(T);
            }
        }

        public static int DefaultPageSize = 200;
        public int PageSize { get; set; } = DefaultPageSize;
        public bool LinearLoad = false;

        public RecordSet() { }
        public RecordSet(IDataAccessor config = null) {
            this.DataAccessor = config;
        }

        public T FirstOrDefault() {
            return this.Count > 0 ? this[0] : default(T);
        }

        public QueryBuilder ListRids() {
            QueryBuilder retv = new QueryBuilder();
            for (int i = 0; i < this.Count; i++) {
                retv.Append(
                    new QueryBuilder(
                        $"@{IntEx.GerarShortRID()}",
                        this[i].RID
                    )
                );
                if (i < this.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }

        public String ListIds() {
            StringBuilder retv = new StringBuilder();
            for (int i = 0; i < this.Count; i++) {
                retv.Append(
                    String.Format(
                        "'{0}'",
                        this[i].Id
                    )
                );
                if (i < this.Count - 1)
                    retv.Append(",");
            }
            return retv.ToString();
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
            var transport = DataAccessor.AggregateLoad(cnd, LimitResults, page, PageSize, OrderingMember, Ordering, LinearLoad);
            foreach(var i in transport) {
                this.Add(i);
            }
            transport = null;

            for(int i = 0; i < this.Count; i++) {
                this[i].DataAccessor = this.DataAccessor;
            }

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
