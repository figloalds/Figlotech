using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {

    public sealed class IntermediateRdbmsLoadAllArgs<T> where T : IDataObject, new() {

        public IntermediateRdbmsLoadAllArgs(BDadosTransaction transaction, LoadAllArgs<T> largs) {
            Transaction = transaction;
            LoadAllArgs = largs;
        }

        private BDadosTransaction Transaction { get; set; }
        private LoadAllArgs<T> LoadAllArgs { get; set; }

        public List<T> Aggregate() {
            return Transaction.DataAccessor.AggregateLoad(Transaction, LoadAllArgs);
        }
        public async Task<List<T>> AggregateAsync() {
            return await Transaction.DataAccessor.AggregateLoadAsync(Transaction, LoadAllArgs);
        }
        public IEnumerable<T> Fetch() {
            return Transaction.DataAccessor.Fetch(Transaction, LoadAllArgs);
        }
        public List<T> Load() {
            return Transaction.DataAccessor.LoadAll(Transaction, LoadAllArgs);
        }
        public async Task<List<T>> LoadAsync() {
            return await Transaction.DataAccessor.LoadAllAsync(Transaction, LoadAllArgs);
        }
    }

    public static class RdbmsLoadAllExtensions {
        public static IntermediateRdbmsLoadAllArgs<T> Using<T>(this LoadAllArgs<T> me, BDadosTransaction transaction) where T: IDataObject, new() {
            return new IntermediateRdbmsLoadAllArgs<T>(transaction, me);
        }
    }
}
