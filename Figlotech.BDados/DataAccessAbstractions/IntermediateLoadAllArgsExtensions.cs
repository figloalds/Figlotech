using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {

    public static class LoadAllArgsExtensions<T> where T : ILegacyDataObject, new() {
        public static IntermediateLoadAllArgsRdbms<T> UsingRdbmsAccessor(LoadAllArgs<T> self, IRdbmsDataAccessor dataAccessor)  {
            return new IntermediateLoadAllArgsRdbms<T>(dataAccessor, self);
        }
    }

    public sealed class IntermediateLoadAllArgsRdbms<T> : IntermediateLoadAllArgs<T> where T : ILegacyDataObject, new() {
        public IntermediateLoadAllArgsRdbms(IRdbmsDataAccessor dataAccessor, LoadAllArgs<T> largs) : base(dataAccessor, largs) {
            DataAccessor = dataAccessor;
            LoadAllArgs = largs;
        }

        private IRdbmsDataAccessor DataAccessor { get; set; }
        private LoadAllArgs<T> LoadAllArgs { get; set; }

        public async Task<List<T>> AggregateAsync() {
            await using var tsn = await DataAccessor.CreateNewTransactionAsync(CancellationToken.None);
            return await DataAccessor.AggregateLoadAsync(tsn, LoadAllArgs);
        }
        public async Task<List<T>> AggregateAsync(BDadosTransaction tsn) {
            return await DataAccessor.AggregateLoadAsync(tsn, LoadAllArgs);
        }
        public async IAsyncEnumerable<T> FetchAsync() {
            await using var tsn = await DataAccessor.CreateNewTransactionAsync(CancellationToken.None);
            await foreach (var item in DataAccessor.FetchAsync(tsn, LoadAllArgs)) {
                yield return item;
            }
        }
        public async IAsyncEnumerable<T> FetchAsync(BDadosTransaction tsn) {
            await foreach (var item in DataAccessor.FetchAsync(tsn, LoadAllArgs)) {
                yield return item;
            }
        }
        public async Task<List<T>> LoadAsync(BDadosTransaction tsn) {
            return await DataAccessor.LoadAllAsync(tsn, LoadAllArgs);
        }
        public async Task<List<T>> LoadAsync() {
            await using var tsn = await DataAccessor.CreateNewTransactionAsync(CancellationToken.None);
            return await DataAccessor.LoadAllAsync(tsn, LoadAllArgs);
        }
    }
}
