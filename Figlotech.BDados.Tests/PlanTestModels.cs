using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Figlotech.BDados.Business;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Interfaces;

namespace Figlotech.BDados.Tests {
    public abstract class PlanDataObject<TIdentifier> : BaseDataObject<TIdentifier> {
        [Field]
        [ReliableId]
        public override TIdentifier Id { get; set; } = default!;
    }

    public sealed class LongRoot : PlanDataObject<long> {
    }

    public static class ThrowingSource {
        public static string ThrowingValue {
            get { throw new NullReferenceException("boom"); }
        }

        public static string ThrowingMethod() {
            throw new NullReferenceException("method boom");
        }
    }

    public class CustomStringLike : IEquatable<CustomStringLike?> {
        public string Value { get; set; }
        public string? Text { get; set; }
        public CustomStringLike? Custom { get; set; }
        public CustomStringLike(string value) { Value = value; Text = value; }
        public bool Contains(string value) => Value.Contains(value);
        public bool StartsWith(string value) => Value.StartsWith(value);
        public bool EndsWith(string value) => Value.EndsWith(value);
        public string ToUpper() => Value.ToUpper();
        public string ToLower() => Value.ToLower();
        public string Trim() => Value.Trim();
        public string Replace(string oldValue, string newValue) => Value.Replace(oldValue, newValue);
        public bool Equals(CustomStringLike? other) => Value.Equals(other?.Value);
        public override bool Equals(object? obj) => obj is CustomStringLike c && Equals(c);
        public override int GetHashCode() => Value.GetHashCode();
    }

    public sealed class GuidRoot : PlanDataObject<Guid> {
        [Field]
        public Guid ScalarAggregateId { get; set; }

        [Field]
        public Guid IntermediateAggregateId { get; set; }

        [Field]
        public Guid ObjectAggregateId { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(ScalarAggregateId), nameof(ScalarAggregate.Name))]
        public string? AggregateName { get; set; }

        [AggregateFarField(typeof(IntermediateAggregate), nameof(IntermediateAggregateId), typeof(FarAggregate), nameof(IntermediateAggregate.FarAggregateId), nameof(FarAggregate.Name))]
        public string? FarAggregateName { get; set; }

        [AggregateObject(nameof(ObjectAggregateId))]
        public ObjectAggregate? AggregateObject { get; set; }

        [AggregateList(typeof(ListAggregate), nameof(ListAggregate.ParentId))]
        public List<ListAggregate> AggregateList { get; set; } = new List<ListAggregate>();
    }

    public sealed class QueryComparisonRoot : PlanDataObject<Guid> {
        [Field]
        [QueryComparison(DataStringComparisonType.ExactValue)]
        public string? ExactValue { get; set; }

        [Field]
        [QueryComparison(DataStringComparisonType.Containing)]
        public string? Containing { get; set; }

        [Field]
        [QueryComparison(DataStringComparisonType.StartingWith)]
        public string? StartingWith { get; set; }

        [Field]
        [QueryComparison(DataStringComparisonType.EndingWith)]
        public string? EndingWith { get; set; }

        [Field]
        [QueryComparison(DataStringComparisonType.IgnoreCase)]
        public string? IgnoreCase { get; set; }
    }

    public sealed class NullableGuidRoot : PlanDataObject<Guid> {
        [Field]
        public Guid? NullableAggregateId { get; set; }
    }

    public sealed class ScalarAggregate : PlanDataObject<Guid> {
        [Field]
        public string? Name { get; set; }
    }

    public sealed class PrivateConstructorScalarRoot : PlanDataObject<Guid> {
        [Field]
        public Guid RemoteScalarId { get; set; }

        [AggregateField(typeof(PrivateConstructorScalarRemote), nameof(RemoteScalarId), nameof(PrivateConstructorScalarRemote.Value))]
        public string? RemoteValue { get; set; }
    }

    public sealed class PrivateConstructorScalarRemote : PlanDataObject<Guid> {
        private PrivateConstructorScalarRemote() {
        }

        [Field]
        public string? Value { get; }
    }

    public sealed class IntermediateAggregate : PlanDataObject<Guid> {
        [Field]
        public Guid FarAggregateId { get; set; }
    }

    public sealed class FarAggregate : PlanDataObject<Guid> {
        [Field]
        public string? Name { get; set; }
    }

    public sealed class ObjectAggregate : PlanDataObject<Guid> {
        [Field]
        public string? Name { get; set; }
    }

    public sealed class ListAggregate : PlanDataObject<Guid> {
        [Field]
        public Guid ParentId { get; set; }

        [Field]
        public string? Name { get; set; }
    }

    public sealed class CyclicAggregateA : PlanDataObject<Guid> {
        [Field]
        public Guid CyclicAggregateBId { get; set; }

        [AggregateObject(nameof(CyclicAggregateBId))]
        public CyclicAggregateB? AggregateB { get; set; }
    }

    public sealed class CyclicAggregateB : PlanDataObject<Guid> {
        [Field]
        public Guid CyclicAggregateAId { get; set; }

        [AggregateObject(nameof(CyclicAggregateAId))]
        public CyclicAggregateA? AggregateA { get; set; }
    }

    public sealed class InvalidAggregateRoot : PlanDataObject<Guid> {
        [Field]
        public string? RootValue { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(Id), "NotASource")]
        public string? Invalid { get; set; }
    }

    public sealed class RepeatedRelationRoot : PlanDataObject<Guid> {
        [Field]
        public Guid ScalarId { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(ScalarId), nameof(ScalarAggregate.Name))]
        public string? FirstName { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(ScalarId), nameof(ScalarAggregate.Name))]
        public string? SecondName { get; set; }
    }

    public sealed class SharedScalarObjectEdgeRoot : PlanDataObject<Guid> {
        [Field]
        public Guid SharedId { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(SharedId), nameof(ScalarAggregate.Name))]
        public string? AggregateName { get; set; }

        [AggregateObject(nameof(SharedId))]
        public ScalarAggregate? Aggregate { get; set; }
    }

    public sealed class MultipleObjectTargetsRoot : PlanDataObject<Guid> {
        [Field]
        public Guid SharedId { get; set; }

        [AggregateObject(nameof(SharedId))]
        public ObjectAggregate? First { get; set; }

        [AggregateObject(nameof(SharedId))]
        public ObjectAggregate? Second { get; set; }
    }

    public sealed class DistinctSiblingRelationshipRoot : PlanDataObject<Guid> {
        [Field]
        public Guid FirstScalarId { get; set; }

        [Field]
        public Guid SecondScalarId { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(FirstScalarId), nameof(ScalarAggregate.Name))]
        public string? FirstName { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(SecondScalarId), nameof(ScalarAggregate.Name))]
        public string? SecondName { get; set; }
    }

    public sealed class InvalidObjectKeyRoot : PlanDataObject<Guid> {
        [AggregateObject("NoSuchKey")]
        public ObjectAggregate? Invalid { get; set; }
    }

    public sealed class InvalidRemoteSourceRoot : PlanDataObject<Guid> {
        [AggregateField(typeof(ScalarAggregate), nameof(Id), "NoSuchSource")]
        public string? Invalid { get; set; }
    }

    public sealed class ReadOnlyAggregateTargetRoot : PlanDataObject<Guid> {
        [AggregateField(typeof(ScalarAggregate), nameof(Id), nameof(ScalarAggregate.Name))]
        public string? Invalid { get; }
    }

    public sealed class ObjectCollectionTargetRoot : PlanDataObject<Guid> {
        [AggregateObject(nameof(Id))]
        public List<ObjectAggregate> Invalid { get; set; } = new List<ObjectAggregate>();
    }

    public sealed class ObjectScalarTargetRoot : PlanDataObject<Guid> {
        [AggregateObject(nameof(Id))]
        public string? Invalid { get; set; }
    }

    public sealed class ObjectNonDataObjectTargetRoot : PlanDataObject<Guid> {
        [AggregateObject(nameof(Id))]
        public NotDataObject? Invalid { get; set; }
    }

    public sealed class NotDataObject {
        public Guid Id { get; set; }
    }

    public sealed class ListElementMismatchRoot : PlanDataObject<Guid> {
        [AggregateList(typeof(ListAggregate), nameof(ListAggregate.ParentId))]
        public List<ObjectAggregate> Invalid { get; set; } = new List<ObjectAggregate>();
    }

    public sealed class InvalidFarImmediateKeyRoot : PlanDataObject<Guid> {
        [AggregateFarField(typeof(IntermediateAggregate), "NoSuchImmediateKey", typeof(FarAggregate), nameof(IntermediateAggregate.FarAggregateId), nameof(FarAggregate.Name))]
        public string? Invalid { get; set; }
    }

    public sealed class InvalidFarKeyRoot : PlanDataObject<Guid> {
        [AggregateFarField(typeof(IntermediateAggregate), nameof(Id), typeof(FarAggregate), "NoSuchFarKey", nameof(FarAggregate.Name))]
        public string? Invalid { get; set; }
    }

    public sealed class InvalidFarSourceRoot : PlanDataObject<Guid> {
        [AggregateFarField(typeof(IntermediateAggregate), nameof(Id), typeof(FarAggregate), nameof(IntermediateAggregate.FarAggregateId), "NoSuchFarSource")]
        public string? Invalid { get; set; }
    }

    public sealed class FlagRoot : PlanDataObject<Guid> {
        [Field]
        public Guid ChildId { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(Id), nameof(ScalarAggregate.Name), Flags = "root")]
        public string? RootOnly { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(Id), nameof(ScalarAggregate.Name), Flags = "child")]
        public string? ChildOnly { get; set; }

        [AggregateObject(nameof(ChildId))]
        public FlagChild? Child { get; set; }
    }

    public sealed class FlagChild : PlanDataObject<Guid> {
        [AggregateField(typeof(ScalarAggregate), nameof(Id), nameof(ScalarAggregate.Name), Flags = "child")]
        public string? ChildOnly { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(Id), nameof(ScalarAggregate.Name), Flags = "root")]
        public string? RootOnly { get; set; }
    }

    public sealed class IdentifierlessListRoot : PlanDataObject<Guid> {
        [AggregateList(typeof(IdentifierlessListChild), nameof(IdentifierlessListChild.ParentId))]
        public List<IdentifierlessListChild> Children { get; set; } = new List<IdentifierlessListChild>();
    }

    public sealed class IdentifierlessListChild : IDataObject {
        object IDataObject.Id { get => null!; set { } }
        public DateTime CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }

        [Field]
        public Guid ParentId { get; set; }
    }

    public sealed class NestedIdentifierlessRoot : PlanDataObject<Guid> {
        [AggregateList(typeof(NestedIdentifierlessParent), nameof(NestedIdentifierlessParent.RootId))]
        public List<NestedIdentifierlessParent> Children { get; set; } = new List<NestedIdentifierlessParent>();
    }

    public sealed class NestedIdentifierlessParent : PlanDataObject<Guid> {
        [Field]
        public Guid RootId { get; set; }

        [Field]
        public Guid GrandchildId { get; set; }

        [AggregateObject(nameof(GrandchildId))]
        public NestedIdentifierlessChild? Grandchild { get; set; }
    }

    public sealed class NestedIdentifierlessChild : IDataObject {
        object IDataObject.Id { get => null!; set { } }
        public DateTime CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }
    }

    public sealed class SharedNestedObjectRoot : PlanDataObject<Guid> {
        [Field]
        public Guid SharedId { get; set; }

        [AggregateObject(nameof(SharedId))]
        public SharedNestedParent? First { get; set; }

        [AggregateObject(nameof(SharedId))]
        public SharedNestedParent? Second { get; set; }
    }

    public sealed class SharedNestedListRoot : PlanDataObject<Guid> {
        [AggregateList(typeof(SharedNestedParent), nameof(SharedNestedParent.RootId))]
        public List<SharedNestedParent> First { get; set; } = new List<SharedNestedParent>();

        [AggregateList(typeof(SharedNestedParent), nameof(SharedNestedParent.RootId))]
        public List<SharedNestedParent> Second { get; set; } = new List<SharedNestedParent>();
    }

    public sealed class SharedNestedParent : PlanDataObject<Guid> {
        [Field]
        public Guid RootId { get; set; }

        [Field]
        public Guid NestedScalarId { get; set; }

        [Field]
        public Guid NestedObjectId { get; set; }

        [AggregateField(typeof(ScalarAggregate), nameof(NestedScalarId), nameof(ScalarAggregate.Name))]
        public string? NestedName { get; set; }

        [AggregateObject(nameof(NestedObjectId))]
        public ObjectAggregate? NestedObject { get; set; }

        [AggregateList(typeof(SharedNestedListItem), nameof(SharedNestedListItem.ParentId))]
        public List<SharedNestedListItem> NestedList { get; set; } = new List<SharedNestedListItem>();
    }

    public sealed class SharedNestedListItem : PlanDataObject<Guid> {
        [Field]
        public Guid ParentId { get; set; }
    }

    public sealed class HighCardinalityRoot : PlanDataObject<Guid> {
        [Field] public Guid Object1Id { get; set; }
        [Field] public Guid Object2Id { get; set; }
        [Field] public Guid Object3Id { get; set; }
        [Field] public Guid Object4Id { get; set; }
        [Field] public Guid Object5Id { get; set; }
        [Field] public Guid Object6Id { get; set; }
        [Field] public Guid Object7Id { get; set; }
        [Field] public Guid Object8Id { get; set; }

        [AggregateObject(nameof(Object1Id))] public HighCardinalityChild? Object1 { get; set; }
        [AggregateObject(nameof(Object2Id))] public HighCardinalityChild? Object2 { get; set; }
        [AggregateObject(nameof(Object3Id))] public HighCardinalityChild? Object3 { get; set; }
        [AggregateObject(nameof(Object4Id))] public HighCardinalityChild? Object4 { get; set; }
        [AggregateObject(nameof(Object5Id))] public HighCardinalityChild? Object5 { get; set; }
        [AggregateObject(nameof(Object6Id))] public HighCardinalityChild? Object6 { get; set; }
        [AggregateObject(nameof(Object7Id))] public HighCardinalityChild? Object7 { get; set; }
        [AggregateObject(nameof(Object8Id))] public HighCardinalityChild? Object8 { get; set; }

        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent1Id))] public List<HighCardinalityChild> List1 { get; set; } = new List<HighCardinalityChild>();
        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent2Id))] public List<HighCardinalityChild> List2 { get; set; } = new List<HighCardinalityChild>();
        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent3Id))] public List<HighCardinalityChild> List3 { get; set; } = new List<HighCardinalityChild>();
        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent4Id))] public List<HighCardinalityChild> List4 { get; set; } = new List<HighCardinalityChild>();
        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent5Id))] public List<HighCardinalityChild> List5 { get; set; } = new List<HighCardinalityChild>();
        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent6Id))] public List<HighCardinalityChild> List6 { get; set; } = new List<HighCardinalityChild>();
        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent7Id))] public List<HighCardinalityChild> List7 { get; set; } = new List<HighCardinalityChild>();
        [AggregateList(typeof(HighCardinalityChild), nameof(HighCardinalityChild.Parent8Id))] public List<HighCardinalityChild> List8 { get; set; } = new List<HighCardinalityChild>();
    }

    public sealed class HighCardinalityChild : PlanDataObject<Guid> {
        [Field] public Guid Parent1Id { get; set; }
        [Field] public Guid Parent2Id { get; set; }
        [Field] public Guid Parent3Id { get; set; }
        [Field] public Guid Parent4Id { get; set; }
        [Field] public Guid Parent5Id { get; set; }
        [Field] public Guid Parent6Id { get; set; }
        [Field] public Guid Parent7Id { get; set; }
        [Field] public Guid Parent8Id { get; set; }
    }

    public abstract class OverrideEffectiveAggregateRootBase : PlanDataObject<Guid> {
        [Field]
        public Guid AggregateId { get; set; }

        [AggregateObject(nameof(AggregateId))]
        public virtual ObjectAggregate? Aggregate { get; set; }
    }

    public sealed class OverrideEffectiveAggregateRoot : OverrideEffectiveAggregateRootBase {
        public override ObjectAggregate? Aggregate { get; set; }
    }

    public abstract class HiddenEffectiveAggregateRootBase : PlanDataObject<Guid> {
        [Field]
        public Guid AggregateId { get; set; }

        [Field]
        public string? ShadowedColumn { get; set; }

        [AggregateObject(nameof(AggregateId))]
        public ObjectAggregate? ShadowedAggregate { get; set; }
    }

    public sealed class HiddenEffectiveAggregateRoot : HiddenEffectiveAggregateRootBase {
        [Field]
        public new string? ShadowedColumn { get; set; }

        public new ObjectAggregate? ShadowedAggregate { get; set; }
    }

    public sealed class RootBoundedCycleParcel : PlanDataObject<Guid> {
        [Field]
        public Guid PaymentId { get; set; }

        [AggregateObject(nameof(PaymentId), Flags = "root")]
        public RootBoundedCyclePayment? Payment { get; set; }
    }

    public sealed class RootBoundedCyclePayment : PlanDataObject<Guid> {
        [AggregateList(typeof(RootBoundedCycleParcel), nameof(RootBoundedCycleParcel.PaymentId))]
        public List<RootBoundedCycleParcel> Parcels { get; set; } = new List<RootBoundedCycleParcel>();
    }

    public sealed class WhitespaceFlagRoot : PlanDataObject<Guid> {
        [AggregateField(typeof(ScalarAggregate), nameof(Id), nameof(ScalarAggregate.Name), Flags = " \t ")]
        public string? Name { get; set; }
    }

    public sealed class UnknownFlagRoot : PlanDataObject<Guid> {
        [AggregateField(typeof(ScalarAggregate), nameof(Id), nameof(ScalarAggregate.Name), Flags = "unknown")]
        public string? Name { get; set; }
    }

    public sealed class RuntimePlanRoot : PlanDataObject<Guid> {
        [Field]
        public Guid ScalarId { get; set; }

        [AggregateField(typeof(RuntimeScalar), nameof(ScalarId), nameof(RuntimeScalar.Name))]
        public string? ScalarName { get; set; }

        [AggregateList(typeof(RuntimeList), nameof(RuntimeList.RootId))]
        public List<RuntimeList> Items { get; set; } = new List<RuntimeList>();
    }

    public sealed class CollidingListKeyRoot : IDataObject {
        [Field]
        [PrimaryKey]
        public Guid ParentReference { get; set; }

        [AggregateList(typeof(CollidingListKeyChild), nameof(CollidingListKeyChild.ParentReference))]
        public List<CollidingListKeyChild> Children { get; set; } = new List<CollidingListKeyChild>();

        object IDataObject.Id {
            get => ParentReference;
            set => ParentReference = (Guid)value;
        }

        public DateTime CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }
    }

    public sealed class CollidingListKeyChild : IDataObject {
        [Field]
        [PrimaryKey]
        public Guid ChildIdentifier { get; set; }

        [Field]
        public Guid ParentReference { get; set; }

        [Field]
        public string? Name { get; set; }

        object IDataObject.Id {
            get => ChildIdentifier;
            set => ChildIdentifier = (Guid)value;
        }

        public DateTime CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }
    }

    public sealed class InvalidListRemoteFieldRoot : PlanDataObject<Guid> {
        [Field]
        public Guid DeceptiveParentField { get; set; }

        [AggregateList(typeof(ListAggregate), nameof(DeceptiveParentField))]
        public List<ListAggregate> Children { get; set; } = new List<ListAggregate>();
    }

    public sealed class RuntimeScalar : PlanDataObject<Guid> {
        [Field]
        public string? Name { get; set; }
    }

    public sealed class RuntimeList : PlanDataObject<Guid> {
        [Field]
        public Guid RootId { get; set; }

        [Field]
        public string? Name { get; set; }
    }

    public sealed class NonAggregateHookLongRoot : PlanDataObject<long>, IBusinessObject<NonAggregateHookLongRoot> {
        public Task<ValidationErrors> ValidateInput() => Task.FromResult<ValidationErrors>(null!);
        public Task<ValidationErrors> ValidateBusiness() => Task.FromResult<ValidationErrors>(null!);
        public Task<string> RunValidations() => Task.FromResult(String.Empty);
        public Task<bool> ValidateAndPersistAsync(string iaToken) => Task.FromResult(false);
        public Task OnBeforePersistAsync() => Task.CompletedTask;
        public Task OnAfterPersistAsync() => Task.CompletedTask;
        public void OnAfterLoad(DataLoadContext ctx) => ((ConcurrentQueue<string>)ctx.ContextTransferObject).Enqueue("load:" + Id);
        public Task OnAfterAggregateLoadAsync(DataLoadContext ctx) {
            ((ConcurrentQueue<string>)ctx.ContextTransferObject).Enqueue("aggregate:" + Id);
            return Task.CompletedTask;
        }
        public Task OnAfterListAggregateLoadAsync(DataLoadContext ctx, List<NonAggregateHookLongRoot> aggregateLoadResult) {
            ((ConcurrentQueue<string>)ctx.ContextTransferObject).Enqueue("list:" + aggregateLoadResult.Count);
            return Task.CompletedTask;
        }
    }

    public sealed class ThrowingHookGuidRoot : PlanDataObject<Guid>, IBusinessObject<ThrowingHookGuidRoot> {
        public Task<ValidationErrors> ValidateInput() => Task.FromResult<ValidationErrors>(null!);
        public Task<ValidationErrors> ValidateBusiness() => Task.FromResult<ValidationErrors>(null!);
        public Task<string> RunValidations() => Task.FromResult(String.Empty);
        public Task<bool> ValidateAndPersistAsync(string iaToken) => Task.FromResult(false);
        public Task OnBeforePersistAsync() => Task.CompletedTask;
        public Task OnAfterPersistAsync() => Task.CompletedTask;
        public void OnAfterLoad(DataLoadContext ctx) { }
        public Task OnAfterAggregateLoadAsync(DataLoadContext ctx) => Task.FromException(new InvalidOperationException("async aggregate hook failure"));
        public Task OnAfterListAggregateLoadAsync(DataLoadContext ctx, List<ThrowingHookGuidRoot> aggregateLoadResult) => Task.CompletedTask;
    }

    public sealed class HookedGuidRoot : PlanDataObject<Guid>, IBusinessObject<HookedGuidRoot> {
        [AggregateList(typeof(ListAggregate), nameof(ListAggregate.ParentId))]
        public List<ListAggregate> AggregateList { get; set; } = new List<ListAggregate>();

        public Task<ValidationErrors> ValidateInput() => Task.FromResult<ValidationErrors>(null!);
        public Task<ValidationErrors> ValidateBusiness() => Task.FromResult<ValidationErrors>(null!);
        public Task<string> RunValidations() => Task.FromResult(String.Empty);
        public Task<bool> ValidateAndPersistAsync(string iaToken) => Task.FromResult(false);
        public Task OnBeforePersistAsync() => Task.CompletedTask;
        public Task OnAfterPersistAsync() => Task.CompletedTask;
        public void OnAfterLoad(DataLoadContext ctx) => ((ConcurrentQueue<string>)ctx.ContextTransferObject).Enqueue("load:" + Id.ToString("D"));
        public Task OnAfterAggregateLoadAsync(DataLoadContext ctx) {
            ((ConcurrentQueue<string>)ctx.ContextTransferObject).Enqueue("aggregate:" + Id.ToString("D"));
            return Task.CompletedTask;
        }
        public Task OnAfterListAggregateLoadAsync(DataLoadContext ctx, List<HookedGuidRoot> aggregateLoadResult) {
            ((ConcurrentQueue<string>)ctx.ContextTransferObject).Enqueue("list:" + aggregateLoadResult.Count);
            return Task.CompletedTask;
        }
    }
}
