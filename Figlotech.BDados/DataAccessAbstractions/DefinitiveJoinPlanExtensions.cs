using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Data;
using System;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public static class DefinitiveJoinPlanExtensions {
        public static DefinitiveJoinPlan Freeze(this JoinDefinition definition, Type rootType, AggregateJoinShape shape) {
            if (definition == null) {
                throw new ArgumentNullException(nameof(definition));
            }
            if (rootType == null) {
                throw new ArgumentNullException(nameof(rootType));
            }
            if (!Enum.IsDefined(typeof(AggregateJoinShape), shape)) {
                throw new ArgumentOutOfRangeException(nameof(shape), shape, "Aggregate join shape must be a defined value.");
            }
            if (definition.Joins == null || definition.Joins.Count == 0) {
                throw new ArgumentException("Join definition must contain at least one table.", nameof(definition));
            }
            return DefinitiveJoinPlanCompiler.Compile(definition, rootType, shape);
        }

        public static DefinitiveJoinPlan BuildPlan(this IJoinBuilder builder, Type rootType, AggregateJoinShape shape) {
            if (builder == null) {
                throw new ArgumentNullException(nameof(builder));
            }
            if (rootType == null) {
                throw new ArgumentNullException(nameof(rootType));
            }
#pragma warning disable CS0618
            JoinDefinition definition = builder.GetJoin();
#pragma warning restore CS0618
            if (definition == null) {
                throw new ArgumentException("Join builder returned a null join definition.", nameof(builder));
            }
            return definition.Freeze(rootType, shape);
        }
    }
}
