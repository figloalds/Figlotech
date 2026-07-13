using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Figlotech.Core.Helpers;

namespace Figlotech.BDados.DataAccessAbstractions {
    /// <summary>
    /// Immutable executable form of a <see cref="DefinitiveJoinPlan"/>. This plan is intentionally
    /// keyed by canonical plan identity rather than its structural-signature diagnostic fingerprint.
    /// </summary>
    public sealed class CompiledAggregateMaterializerPlan {
        private static readonly ConditionalWeakTable<DefinitiveJoinPlan, CompiledAggregateMaterializerPlan> _cache = new ConditionalWeakTable<DefinitiveJoinPlan, CompiledAggregateMaterializerPlan>();

        private readonly TableExecutionPlan[] _tables;
        private readonly RelationExecutionPlan[][] _relationsByParent;
        private readonly int _projectionLength;
        private readonly int _rootTableIndex;
        private readonly WeakReference<DefinitiveJoinPlan> _sourcePlan;

        private CompiledAggregateMaterializerPlan(DefinitiveJoinPlan plan) {
            _sourcePlan = new WeakReference<DefinitiveJoinPlan>(plan);
            RootType = plan.RootType;
            Shape = plan.Shape;
            TableCount = plan.Tables.Length;
            PlanStructuralSignature = plan.StructuralSignature;
            _projectionLength = plan.Projection.Length;
            _rootTableIndex = plan.RootTableIndex;
            _tables = BuildTables(plan);
            _relationsByParent = BuildRelations(plan, _tables);
        }

        public string PlanStructuralSignature { get; }
        public Type RootType { get; }
        public AggregateJoinShape Shape { get; }
        public int TableCount { get; }

        internal int ProjectionLength => _projectionLength;

        public static CompiledAggregateMaterializerPlan GetOrCreate(DefinitiveJoinPlan plan) {
            if (plan == null) {
                throw new ArgumentNullException(nameof(plan));
            }
            return _cache.GetValue(plan, key => new CompiledAggregateMaterializerPlan(key));
        }

        public ImmutableArray<T> Materialize<T>(IEnumerable<object[]> rows) {
            if (rows == null) {
                throw new ArgumentNullException(nameof(rows));
            }
            ValidateRootType<T>();

            CompiledAggregateMaterializationContext context = CreateContext();
            var results = new List<T>();
            int currentRow = 0;
            foreach (object[] row in rows) {
                object rootIdentifier = ReadRootIdentifier(row, currentRow);
                if (rootIdentifier == null) {
                    currentRow++;
                    continue;
                }

                object root = GetOrCreateRoot(rootIdentifier, context, out bool isNew);
                if (isNew) {
                    results.Add((T)root);
                }
                PopulateRoot(row, root, isNew, context);
                currentRow++;
            }

            return ImmutableArray.CreateRange(results);
        }

        internal void ValidateSourcePlan(DefinitiveJoinPlan plan) {
            if (plan == null) {
                throw new ArgumentNullException(nameof(plan));
            }
            if (!_sourcePlan.TryGetTarget(out DefinitiveJoinPlan source) || !ReferenceEquals(source, plan)) {
                throw new ArgumentException("Compiled aggregate materializer must be used with the same definitive source plan instance from which it was compiled.", nameof(plan));
            }
        }

        internal void ValidateRootType<T>() {
            if (typeof(T) != RootType) {
                throw new ArgumentException($"Compiled aggregate plan root type is '{RootType.FullName}', not requested materialization type '{typeof(T).FullName}'.", nameof(T));
            }
        }

        internal object ReadRootIdentifier(object[] row, int rowIndex) {
            ValidateRow(row, rowIndex);
            return _tables[_rootTableIndex].ReadIdentifier(row);
        }

        internal CompiledAggregateMaterializationContext CreateContext() {
            return new CompiledAggregateMaterializationContext();
        }

        internal object GetOrCreateRoot(object identifier, CompiledAggregateMaterializationContext context, out bool isNew) {
            if (identifier == null) {
                throw new ArgumentNullException(nameof(identifier));
            }
            if (context == null) {
                throw new ArgumentNullException(nameof(context));
            }
            TableExecutionPlan rootTable = _tables[_rootTableIndex];
            return context.GetOrCreate(rootTable.Index, identifier, rootTable.CreateInstance, out isNew);
        }

        internal void PopulateRoot(object[] row, object root, bool isNew, CompiledAggregateMaterializationContext context) {
            Populate(row, root, _rootTableIndex, isNew, context, 0);
        }

        internal void Populate(object[] row, object target, int tableIndex, bool isNew, CompiledAggregateMaterializationContext context, int recursionDepth) {
            if (row == null) {
                throw new ArgumentNullException(nameof(row));
            }
            if (target == null) {
                throw new ArgumentNullException(nameof(target));
            }
            if (context == null) {
                throw new ArgumentNullException(nameof(context));
            }
            if (tableIndex < 0 || tableIndex >= _tables.Length) {
                throw new ArgumentOutOfRangeException(nameof(tableIndex), tableIndex, "Table index is outside the compiled aggregate plan.");
            }
            if (recursionDepth > _tables.Length) {
                throw new InvalidOperationException("Aggregate materialization exceeded the compiled plan depth; the aggregate graph contains an execution cycle.");
            }

            TableExecutionPlan table = _tables[tableIndex];
            if (!table.EntityType.IsInstanceOfType(target)) {
                throw new ArgumentException($"Target type '{target.GetType().FullName}' does not match compiled table type '{table.EntityType.FullName}'.", nameof(target));
            }
            if (isNew) {
                table.PopulateFields(target, row);
            }

            RelationExecutionPlan[] relations = _relationsByParent[tableIndex];
            for (int i = 0; i < relations.Length; i++) {
                RelationExecutionPlan relation = relations[i];
                switch (relation.BuildKind) {
                    case AggregateBuildOptions.AggregateField:
                        if (isNew) {
                            relation.SetField(target, row[relation.SourceOrdinal]);
                        }
                        break;

                    case AggregateBuildOptions.AggregateObject:
                        MaterializeObjectRelation(row, target, relation, context, recursionDepth);
                        break;

                    case AggregateBuildOptions.AggregateList:
                        MaterializeListRelation(row, target, relation, context, recursionDepth);
                        break;
                }
            }
        }

        private void MaterializeObjectRelation(object[] row, object parent, RelationExecutionPlan relation, CompiledAggregateMaterializationContext context, int recursionDepth) {
            TableExecutionPlan childTable = _tables[relation.ChildTableIndex];
            object childIdentifier = childTable.ReadIdentifier(row);
            if (childIdentifier == null) {
                return;
            }

            object child = context.GetOrCreate(childTable.Index, childIdentifier, childTable.CreateInstance, out bool childIsNew);
            relation.SetObject(parent, child);
            Populate(row, child, childTable.Index, childIsNew, context, recursionDepth + 1);
        }

        private void MaterializeListRelation(object[] row, object parent, RelationExecutionPlan relation, CompiledAggregateMaterializationContext context, int recursionDepth) {
            TableExecutionPlan childTable = _tables[relation.ChildTableIndex];
            object childIdentifier = childTable.ReadIdentifier(row);
            if (childIdentifier == null) {
                return;
            }

            object child = context.GetOrCreate(childTable.Index, childIdentifier, childTable.CreateInstance, out bool childIsNew);
            object list = context.GetOrCreateList(parent, relation.Index, relation.GetList, relation.CreateListInstance, relation.SetList);

            if (context.TryAddListMember(parent, relation.Index, childIdentifier)) {
                relation.AddToList(list, child);
            }
            Populate(row, child, childTable.Index, childIsNew, context, recursionDepth + 1);
        }

        private void ValidateRow(object[] row, int rowIndex) {
            if (row == null) {
                throw new ArgumentException($"Materialization row at index {rowIndex} cannot be null.", nameof(row));
            }
            if (row.Length < _projectionLength) {
                throw new ArgumentException($"Materialization row at index {rowIndex} has length {row.Length}, but compiled projection requires at least {_projectionLength} values.", nameof(row));
            }
        }

        private static TableExecutionPlan[] BuildTables(DefinitiveJoinPlan plan) {
            var requiresInstance = new bool[plan.Tables.Length];
            requiresInstance[plan.RootTableIndex] = true;
            for (int relationIndex = 0; relationIndex < plan.Relations.Length; relationIndex++) {
                DefinitiveJoinRelation relation = plan.Relations[relationIndex];
                if (relation.BuildKind == AggregateBuildOptions.AggregateObject || relation.BuildKind == AggregateBuildOptions.AggregateList) {
                    requiresInstance[relation.ChildTableIndex] = true;
                }
            }

            var tables = new TableExecutionPlan[plan.Tables.Length];
            for (int tableIndex = 0; tableIndex < plan.Tables.Length; tableIndex++) {
                DefinitiveJoinTable source = plan.Tables[tableIndex];
                var columns = new List<DefinitiveProjectionColumn>();
                for (int projectionIndex = 0; projectionIndex < plan.Projection.Length; projectionIndex++) {
                    DefinitiveProjectionColumn column = plan.Projection[projectionIndex];
                    if (column.TableIndex == tableIndex) {
                        columns.Add(column);
                    }
                }

                Func<object> createInstance = null;
                if (requiresInstance[tableIndex]) {
                    createInstance = BuildConstructor(source.EntityType);
                }
                tables[tableIndex] = new TableExecutionPlan(
                    tableIndex,
                    source.EntityType,
                    createInstance,
                    BuildIdentifierReader(source.Identifier),
                    BuildFieldPopulator(source.EntityType, columns));
            }
            return tables;
        }

        private static RelationExecutionPlan[][] BuildRelations(DefinitiveJoinPlan plan, TableExecutionPlan[] tables) {
            var grouped = new List<RelationExecutionPlan>[tables.Length];
            for (int i = 0; i < grouped.Length; i++) {
                grouped[i] = new List<RelationExecutionPlan>();
            }

            for (int relationIndex = 0; relationIndex < plan.Relations.Length; relationIndex++) {
                DefinitiveJoinRelation source = plan.Relations[relationIndex];
                if (source.BuildKind == AggregateBuildOptions.None) {
                    continue;
                }

                Type parentType = tables[source.ParentTableIndex].EntityType;
                Type childType = tables[source.ChildTableIndex].EntityType;
                switch (source.BuildKind) {
                    case AggregateBuildOptions.AggregateField:
                        if (source.SourceFields.Length != 1) {
                            throw new ArgumentException($"Aggregate field relation at index {relationIndex} must specify exactly one source field.", nameof(plan));
                        }
                        grouped[source.ParentTableIndex].Add(RelationExecutionPlan.ForField(
                            relationIndex,
                            source.ChildTableIndex,
                            FindProjectionOrdinal(plan, source.ChildTableIndex, source.SourceFields[0], relationIndex),
                            BuildConvertingSetter(parentType, source.TargetMember, relationIndex)));
                        break;

                    case AggregateBuildOptions.AggregateObject:
                        EnsureObjectTarget(parentType, childType, source.TargetMember, relationIndex);
                        grouped[source.ParentTableIndex].Add(RelationExecutionPlan.ForObject(
                            relationIndex,
                            source.ChildTableIndex,
                            BuildSetter(parentType, source.TargetMember, relationIndex)));
                        break;

                    case AggregateBuildOptions.AggregateList:
                        Type listType = EnsureListTarget(parentType, childType, source.TargetMember, relationIndex);
                        grouped[source.ParentTableIndex].Add(RelationExecutionPlan.ForList(
                            relationIndex,
                            source.ChildTableIndex,
                            BuildSetter(parentType, source.TargetMember, relationIndex),
                            BuildGetter(parentType, source.TargetMember, relationIndex),
                            BuildConstructor(listType),
                            BuildListAdd(listType, childType, relationIndex)));
                        break;

                    default:
                        throw new ArgumentException($"Aggregate relation at index {relationIndex} has unsupported build kind '{source.BuildKind}'.", nameof(plan));
                }
            }

            var result = new RelationExecutionPlan[grouped.Length][];
            for (int i = 0; i < grouped.Length; i++) {
                result[i] = grouped[i].ToArray();
            }
            return result;
        }

        private static int FindProjectionOrdinal(DefinitiveJoinPlan plan, int tableIndex, string sourceField, int relationIndex) {
            for (int i = 0; i < plan.Projection.Length; i++) {
                DefinitiveProjectionColumn projection = plan.Projection[i];
                if (projection.TableIndex == tableIndex && String.Equals(projection.SourceColumn, sourceField, StringComparison.Ordinal)) {
                    return projection.Ordinal;
                }
            }
            throw new ArgumentException($"Aggregate relation at index {relationIndex} source field '{sourceField}' is not projected by child table index {tableIndex}.", nameof(plan));
        }

        private static Type EnsureObjectTarget(Type parentType, Type childType, MemberInfo targetMember, int relationIndex) {
            Type targetType = GetWritableMemberType(parentType, targetMember, relationIndex);
            if (targetType != childType) {
                throw new ArgumentException($"Aggregate object relation at index {relationIndex} target member '{targetMember.Name}' on '{parentType.FullName}' must have type '{childType.FullName}', but is '{targetType.FullName}'.");
            }
            return targetType;
        }

        private static Type EnsureListTarget(Type parentType, Type childType, MemberInfo targetMember, int relationIndex) {
            Type targetType = GetWritableMemberType(parentType, targetMember, relationIndex);
            if (!targetType.IsGenericType || targetType.GetGenericArguments().Length != 1 || targetType.GetGenericArguments()[0] != childType) {
                throw new ArgumentException($"Aggregate list relation at index {relationIndex} target member '{targetMember.Name}' on '{parentType.FullName}' must be a generic collection of '{childType.FullName}'.");
            }
            return targetType;
        }

        private static Type GetWritableMemberType(Type ownerType, MemberInfo member, int relationIndex) {
            if (member == null) {
                throw new ArgumentException($"Aggregate relation at index {relationIndex} has no target member.");
            }
            if (member.DeclaringType == null || !member.DeclaringType.IsAssignableFrom(ownerType)) {
                string declaringType = member.DeclaringType?.FullName ?? "<unknown>";
                throw new ArgumentException($"Aggregate relation at index {relationIndex} target member '{member.Name}' declared on '{declaringType}' cannot target parent table type '{ownerType.FullName}'.");
            }
            if (TryGetWritableMemberType(member, out Type memberType)) {
                return memberType;
            }
            throw new ArgumentException($"Aggregate relation at index {relationIndex} target member '{member.Name}' on '{ownerType.FullName}' must be writable.");
        }

        private static bool TryGetWritableMemberType(MemberInfo member, out Type memberType) {
            if (member is PropertyInfo property && property.SetMethod != null && property.GetIndexParameters().Length == 0) {
                memberType = property.PropertyType;
                return true;
            }
            if (member is FieldInfo field && !field.IsInitOnly && !field.IsLiteral) {
                memberType = field.FieldType;
                return true;
            }
            memberType = null;
            return false;
        }

        private static Func<object> BuildConstructor(Type type) {
            if (!type.IsValueType && type.GetConstructor(Type.EmptyTypes) == null) {
                throw new ArgumentException($"Compiled aggregate materialization requires a public parameterless constructor on '{type.FullName}'.", nameof(type));
            }
            Expression create = Expression.New(type);
            return Expression.Lambda<Func<object>>(Expression.Convert(create, typeof(object))).Compile();
        }

        private static Func<object[], object> BuildIdentifierReader(DefinitiveIdentifier identifier) {
            var row = Expression.Parameter(typeof(object[]), "row");
            Expression raw = Expression.ArrayIndex(row, Expression.Constant(identifier.ProjectionOrdinal));
            Expression converted = ReflectionTool.BuildObjectToTargetConversionExpression(BuildNormalizedProviderValue(raw, identifier.ClrType), identifier.ClrType);
            if (converted.Type != identifier.ClrType) {
                converted = Expression.Convert(converted, identifier.ClrType);
            }
            Expression body = Expression.Condition(
                BuildIsNullOrDbNull(raw),
                Expression.Constant(null, typeof(object)),
                Expression.Convert(converted, typeof(object)));
            return Expression.Lambda<Func<object[], object>>(body, row).Compile();
        }

        private static Action<object, object[]> BuildFieldPopulator(Type ownerType, List<DefinitiveProjectionColumn> columns) {
            var target = Expression.Parameter(typeof(object), "target");
            var row = Expression.Parameter(typeof(object[]), "row");
            var typedTarget = Expression.Variable(ownerType, "typedTarget");
            var expressions = new List<Expression> {
                Expression.Assign(typedTarget, Expression.Convert(target, ownerType))
            };

            for (int i = 0; i < columns.Count; i++) {
                DefinitiveProjectionColumn column = columns[i];
                if (column.DestinationMember == null) {
                    continue;
                }
                if (column.DestinationMember.DeclaringType == null || !column.DestinationMember.DeclaringType.IsAssignableFrom(ownerType)) {
                    string declaringType = column.DestinationMember.DeclaringType?.FullName ?? "<unknown>";
                    throw new ArgumentException($"Projection column at ordinal {column.Ordinal} destination member '{column.DestinationMember.Name}' declared on '{declaringType}' cannot target table type '{ownerType.FullName}'.");
                }
                if (!TryGetWritableMemberType(column.DestinationMember, out Type memberType)) {
                    continue;
                }
                Expression raw = Expression.ArrayIndex(row, Expression.Constant(column.Ordinal));
                Expression converted = ReflectionTool.BuildObjectToTargetConversionExpression(BuildNormalizedProviderValue(raw, memberType), memberType);
                if (converted.Type != memberType) {
                    converted = Expression.Convert(converted, memberType);
                }
                Expression assignment = Expression.Assign(BuildMemberAccess(typedTarget, column.DestinationMember), converted);
                if (memberType.IsValueType && Nullable.GetUnderlyingType(memberType) == null) {
                    expressions.Add(Expression.IfThen(Expression.Not(BuildIsNullOrDbNull(raw)), assignment));
                } else {
                    expressions.Add(assignment);
                }
            }

            expressions.Add(Expression.Empty());
            Expression body = Expression.Block(new[] { typedTarget }, expressions);
            return Expression.Lambda<Action<object, object[]>>(body, target, row).Compile();
        }

        private static Action<object, object> BuildConvertingSetter(Type ownerType, MemberInfo member, int relationIndex) {
            Type memberType = GetWritableMemberType(ownerType, member, relationIndex);
            var target = Expression.Parameter(typeof(object), "target");
            var value = Expression.Parameter(typeof(object), "value");
            Expression converted = ReflectionTool.BuildObjectToTargetConversionExpression(BuildNormalizedProviderValue(value, memberType), memberType);
            if (converted.Type != memberType) {
                converted = Expression.Convert(converted, memberType);
            }
            Expression assignment = Expression.Assign(BuildMemberAccess(Expression.Convert(target, ownerType), member), converted);
            Expression body = memberType.IsValueType && Nullable.GetUnderlyingType(memberType) == null
                ? Expression.IfThen(Expression.Not(BuildIsNullOrDbNull(value)), assignment)
                : assignment;
            return Expression.Lambda<Action<object, object>>(body, target, value).Compile();
        }

        private static Action<object, object> BuildSetter(Type ownerType, MemberInfo member, int relationIndex) {
            Type memberType = GetWritableMemberType(ownerType, member, relationIndex);
            var target = Expression.Parameter(typeof(object), "target");
            var value = Expression.Parameter(typeof(object), "value");
            Expression assignment = Expression.Assign(BuildMemberAccess(Expression.Convert(target, ownerType), member), Expression.Convert(value, memberType));
            return Expression.Lambda<Action<object, object>>(assignment, target, value).Compile();
        }

        private static Func<object, object> BuildGetter(Type ownerType, MemberInfo member, int relationIndex) {
            GetWritableMemberType(ownerType, member, relationIndex);
            var target = Expression.Parameter(typeof(object), "target");
            Expression access = BuildMemberAccess(Expression.Convert(target, ownerType), member);
            return Expression.Lambda<Func<object, object>>(Expression.Convert(access, typeof(object)), target).Compile();
        }

        private static Action<object, object> BuildListAdd(Type listType, Type elementType, int relationIndex) {
            MethodInfo add = listType.GetMethod("Add", new[] { elementType });
            if (add == null) {
                throw new ArgumentException($"Aggregate list relation at index {relationIndex} target collection '{listType.FullName}' must expose Add({elementType.FullName}).");
            }
            var list = Expression.Parameter(typeof(object), "list");
            var item = Expression.Parameter(typeof(object), "item");
            Expression call = Expression.Call(Expression.Convert(list, listType), add, Expression.Convert(item, elementType));
            return Expression.Lambda<Action<object, object>>(call, list, item).Compile();
        }

        private static MemberExpression BuildMemberAccess(Expression target, MemberInfo member) {
            if (member is PropertyInfo property) {
                return Expression.Property(target, property);
            }
            if (member is FieldInfo field) {
                return Expression.Field(target, field);
            }
            throw new ArgumentException("Aggregate materialization members must be fields or properties.", nameof(member));
        }

        private static Expression BuildIsNullOrDbNull(Expression value) {
            return Expression.OrElse(
                Expression.Equal(value, Expression.Constant(null, typeof(object))),
                Expression.ReferenceEqual(value, Expression.Field(null, typeof(DBNull).GetField(nameof(DBNull.Value)))));
        }

        private static Expression BuildNormalizedProviderValue(Expression value, Type targetType) {
            return Expression.Call(
                typeof(CompiledAggregateMaterializerPlan),
                nameof(NormalizeProviderValue),
                null,
                value,
                Expression.Constant(targetType, typeof(Type)));
        }

        private static object NormalizeProviderValue(object value, Type targetType) {
            Type effectiveType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (effectiveType == typeof(Guid) && value is string text && Guid.TryParse(text, out Guid guid)) {
                return guid;
            }
            return value;
        }

        private sealed class TableExecutionPlan {
            public TableExecutionPlan(int index, Type entityType, Func<object> createInstance, Func<object[], object> readIdentifier, Action<object, object[]> populateFields) {
                Index = index;
                EntityType = entityType;
                CreateInstance = createInstance;
                ReadIdentifier = readIdentifier;
                PopulateFields = populateFields;
            }

            public int Index { get; }
            public Type EntityType { get; }
            public Func<object> CreateInstance { get; }
            public Func<object[], object> ReadIdentifier { get; }
            public Action<object, object[]> PopulateFields { get; }
        }

        private sealed class RelationExecutionPlan {
            private RelationExecutionPlan(int index, AggregateBuildOptions buildKind, int childTableIndex) {
                Index = index;
                BuildKind = buildKind;
                ChildTableIndex = childTableIndex;
            }

            public int Index { get; }
            public AggregateBuildOptions BuildKind { get; }
            public int ChildTableIndex { get; }
            public int SourceOrdinal { get; private set; }
            public Action<object, object> SetField { get; private set; }
            public Action<object, object> SetObject { get; private set; }
            public Action<object, object> SetList { get; private set; }
            public Func<object, object> GetList { get; private set; }
            public Func<object> CreateListInstance { get; private set; }
            public Action<object, object> AddToList { get; private set; }

            public static RelationExecutionPlan ForField(int index, int childTableIndex, int sourceOrdinal, Action<object, object> setField) {
                return new RelationExecutionPlan(index, AggregateBuildOptions.AggregateField, childTableIndex) {
                    SourceOrdinal = sourceOrdinal,
                    SetField = setField
                };
            }

            public static RelationExecutionPlan ForObject(int index, int childTableIndex, Action<object, object> setObject) {
                return new RelationExecutionPlan(index, AggregateBuildOptions.AggregateObject, childTableIndex) {
                    SetObject = setObject
                };
            }

            public static RelationExecutionPlan ForList(int index, int childTableIndex, Action<object, object> setList, Func<object, object> getList, Func<object> createListInstance, Action<object, object> addToList) {
                return new RelationExecutionPlan(index, AggregateBuildOptions.AggregateList, childTableIndex) {
                    SetList = setList,
                    GetList = getList,
                    CreateListInstance = createListInstance,
                    AddToList = addToList
                };
            }
        }
    }

    internal sealed class CompiledAggregateMaterializationContext {
        private readonly Dictionary<(int TableIndex, object Identifier), object> _objects = new Dictionary<(int TableIndex, object Identifier), object>();
        private readonly ConditionalWeakTable<object, ParentRelationState> _listMembership = new ConditionalWeakTable<object, ParentRelationState>();

        public object GetOrCreate(int tableIndex, object identifier, Func<object> factory, out bool isNew) {
            var key = (tableIndex, identifier);
            if (_objects.TryGetValue(key, out object existing)) {
                isNew = false;
                return existing;
            }

            object created = factory();
            _objects.Add(key, created);
            isNew = true;
            return created;
        }

        public object GetOrCreateList(object parent, int relationIndex, Func<object, object> getList, Func<object> createList, Action<object, object> setList) {
            ParentRelationState state = _listMembership.GetValue(parent, _ => new ParentRelationState());
            if (state.TryGetList(relationIndex, out object existing)) {
                return existing;
            }

            object list = getList(parent);
            if (list == null) {
                list = createList();
                setList(parent, list);
            }
            state.SetList(relationIndex, list);
            return list;
        }

        public bool TryAddListMember(object parent, int relationIndex, object childIdentifier) {
            ParentRelationState state = _listMembership.GetValue(parent, _ => new ParentRelationState());
            return state.TryAdd(relationIndex, childIdentifier);
        }

        private sealed class ParentRelationState {
            private readonly Dictionary<int, object> _listsByRelation = new Dictionary<int, object>();
            private readonly Dictionary<int, HashSet<object>> _membersByRelation = new Dictionary<int, HashSet<object>>();

            public bool TryGetList(int relationIndex, out object list) {
                return _listsByRelation.TryGetValue(relationIndex, out list);
            }

            public void SetList(int relationIndex, object list) {
                _listsByRelation.Add(relationIndex, list);
            }

            public bool TryAdd(int relationIndex, object childIdentifier) {
                if (!_membersByRelation.TryGetValue(relationIndex, out HashSet<object> members)) {
                    members = new HashSet<object>();
                    _membersByRelation.Add(relationIndex, members);
                }
                return members.Add(childIdentifier);
            }
        }
    }
}
