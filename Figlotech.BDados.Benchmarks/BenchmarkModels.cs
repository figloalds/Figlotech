using System;
using System.Collections.Generic;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;

namespace Figlotech.BDados.Benchmarks {
    public abstract class BenchmarkRow<TIdentifier> : BaseDataObject<TIdentifier> {
        [Field]
        [ReliableId]
        public override TIdentifier Id { get; set; } = default!;

        [Field]
        public int? Value { get; set; }
    }

    public sealed class Int32BenchmarkRow : BenchmarkRow<int> { }
    public sealed class Int64BenchmarkRow : BenchmarkRow<long> { }
    public sealed class GuidBenchmarkRow : BenchmarkRow<Guid> { }
    public sealed class StringBenchmarkRow : BenchmarkRow<string> { }

    public sealed class SmallCompileRoot : BenchmarkRow<long> { }

    public sealed class MediumCompileRoot : BenchmarkRow<long> {
        [Field]
        public long ReferenceId { get; set; }

        [AggregateField(typeof(CompileReference), nameof(ReferenceId), nameof(CompileReference.Name))]
        public string? ReferenceName { get; set; }
    }

    public sealed class LargeCompileRoot : BenchmarkRow<long> {
        [Field]
        public long ReferenceId { get; set; }

        [Field]
        public long ChildId { get; set; }

        [AggregateField(typeof(CompileReference), nameof(ReferenceId), nameof(CompileReference.Name))]
        public string? ReferenceName { get; set; }

        [AggregateObject(nameof(ChildId))]
        public CompileChild? Child { get; set; }

        [AggregateList(typeof(CompileChild), nameof(CompileChild.ParentId))]
        public List<CompileChild> Children { get; set; } = new List<CompileChild>();
    }

    public sealed class CompileReference : BenchmarkRow<long> {
        [Field]
        public string? Name { get; set; }
    }

    public sealed class CompileChild : BenchmarkRow<long> {
        [Field]
        public long ParentId { get; set; }
    }

    public enum BenchmarkIdentifierKind {
        Int32,
        Int64,
        Guid,
        String
    }

    public enum CompileGraphSize {
        Small,
        Medium,
        Large
    }
}
