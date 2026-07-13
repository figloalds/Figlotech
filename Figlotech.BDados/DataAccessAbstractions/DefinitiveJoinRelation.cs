using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class DefinitiveJoinRelation {
        public DefinitiveJoinRelation(
            int parentTableIndex,
            int childTableIndex,
            string parentKey,
            string childKey,
            AggregateBuildOptions buildKind,
            MemberInfo targetMember,
            IEnumerable<string> sourceFields) {
            if (parentTableIndex < 0) {
                throw new ArgumentOutOfRangeException(nameof(parentTableIndex), parentTableIndex, "Parent table index must be non-negative.");
            }
            if (childTableIndex < 0) {
                throw new ArgumentOutOfRangeException(nameof(childTableIndex), childTableIndex, "Child table index must be non-negative.");
            }
            if (sourceFields == null) {
                throw new ArgumentNullException(nameof(sourceFields));
            }

            ParentTableIndex = parentTableIndex;
            ChildTableIndex = childTableIndex;
            ParentKey = DefinitivePlanValidation.RequireText(parentKey, nameof(parentKey));
            ChildKey = DefinitivePlanValidation.RequireText(childKey, nameof(childKey));
            if (targetMember == null) {
                if (buildKind == AggregateBuildOptions.AggregateField
                    || buildKind == AggregateBuildOptions.AggregateObject
                    || buildKind == AggregateBuildOptions.AggregateList) {
                    throw new ArgumentNullException(nameof(targetMember), "Aggregate relations require a target member.");
                }
            } else {
                ValidateTargetMember(targetMember);
            }
            BuildKind = buildKind;
            TargetMember = targetMember;
            SourceFields = ImmutableArray.CreateRange(sourceFields);
            for (int i = 0; i < SourceFields.Length; i++) {
                DefinitivePlanValidation.RequireText(SourceFields[i], nameof(sourceFields));
            }
        }

        private static void ValidateTargetMember(MemberInfo targetMember) {
            if (targetMember is PropertyInfo property) {
                if (property.SetMethod != null && property.GetIndexParameters().Length == 0) {
                    return;
                }
            } else if (targetMember is FieldInfo field) {
                if (!field.IsLiteral && !field.IsInitOnly) {
                    return;
                }
            }

            throw new ArgumentException("Aggregate target member must be a writable, non-indexed property or mutable field.", nameof(targetMember));
        }

        public int ParentTableIndex { get; }
        public int ChildTableIndex { get; }
        public string ParentKey { get; }
        public string ChildKey { get; }
        public AggregateBuildOptions BuildKind { get; }
        public MemberInfo TargetMember { get; }
        public ImmutableArray<string> SourceFields { get; }
    }
}
