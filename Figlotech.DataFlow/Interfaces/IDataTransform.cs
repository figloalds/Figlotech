using Figlotech.DataFlow.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.DataFlow.Interfaces
{
    public abstract class DataTransform : IDataTransform {
        protected IDataTransform Input { get; private set; }
        public object[] Current { get; protected set; }

        public async Task Initialize() {
            await Input.Initialize();
        }
        public void Dispose() {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool d) {
            if (d) {
                Input.Dispose();
            }
        }

        public virtual async Task<bool> Next() {
            if (await Input.Next()) {
                this.Current = ProcessInput(Input.Current);
                return true;
            } else {
                return false;
            }
        }
        public abstract Task<string[]> GetHeaders();
        public abstract object[] ProcessInput(object[] input);

        internal void SetInput(IDataTransform input) {
            Input = input;
        }

        public IEnumerator<object[]> GetEnumerator() {
            return new DataTransformEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return new DataTransformEnumerator(this);
        }
    }

    public sealed class  DataTransformEnumerator : IEnumerator<object[]> {
        IDataTransform Source { get; set; }
        public DataTransformEnumerator(IDataTransform source) {
            Source = source;
            Source.Initialize().GetAwaiter().GetResult();
            Source.GetHeaders().GetAwaiter().GetResult();
        }

        public object[] Current => Source.Current;

        object IEnumerator.Current => Source;

        public void Dispose() {
            Source.Dispose();
        }

        public bool MoveNext() {
            return Source.Next().GetAwaiter().GetResult();
        }

        public void Reset() {
            Source.Initialize().GetAwaiter().GetResult();
        }
    }

    public static class IDataTransformExtensions {
        public static DataTransform Pipe(this IDataTransform self, DataTransform other) {
            other.SetInput(self);
            return other;
        }
    }

    public interface IDataTransform : IDisposable, IEnumerable<object[]>
    {
        Task Initialize();
        Task<bool> Next();
        Task<string[]> GetHeaders();

        object[] Current { get; }
    }
}
