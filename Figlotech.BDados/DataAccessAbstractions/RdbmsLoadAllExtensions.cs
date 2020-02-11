using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {

    public class IntermediateRdbmsLoadAllArgs<T> where T : IDataObject, new() {

        public IntermediateRdbmsLoadAllArgs(BDadosTransaction transaction, LoadAllArgs<T> largs) {
            Transaction = transaction;
            LoadAllArgs = largs;
        }

        private BDadosTransaction Transaction { get; set; }
        private LoadAllArgs<T> LoadAllArgs { get; set; }

        public List<T> Aggregate() {
            return Transaction.DataAccessor.AggregateLoad(Transaction, LoadAllArgs);
        }
        public IEnumerable<T> Fetch() {
            return Transaction.DataAccessor.Fetch(Transaction, LoadAllArgs);
        }
        public List<T> Load() {
            return Transaction.DataAccessor.LoadAll(Transaction, LoadAllArgs);
        }
    }

    public static class RdbmsLoadAllExtensions {
        public static IntermediateRdbmsLoadAllArgs<T> Using<T>(this LoadAllArgs<T> me, BDadosTransaction transaction) where T: IDataObject, new() {
            return new IntermediateRdbmsLoadAllArgs<T>(transaction, me);
        }
    }
}
