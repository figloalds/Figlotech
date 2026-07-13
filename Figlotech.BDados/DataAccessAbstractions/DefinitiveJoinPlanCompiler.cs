using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Interfaces;

namespace Figlotech.BDados.DataAccessAbstractions {
public static class DefinitiveJoinPlanCompiler {
    private sealed class AutomaticCompilation {
        private sealed class AggregateMetadata {
            public MemberInfo Member { get; }

            public AggregateFieldAttribute Field { get; }

            public AggregateFarFieldAttribute FarField { get; }

            public AggregateObjectAttribute Object { get; }

            public AggregateListAttribute List { get; }

            public AggregateMetadata(MemberInfo member, AggregateFieldAttribute field, AggregateFarFieldAttribute farField, AggregateObjectAttribute aggregateObject, AggregateListAttribute list) {
                Member = member;
                Field = field;
                FarField = farField;
                Object = aggregateObject;
                List = list;
            }
        }

        private readonly struct JoinKeys {
            public string ParentKey { get; }

            public string ChildKey { get; }

            public JoinKeys(string parentKey, string childKey) {
                ParentKey = parentKey;
                ChildKey = childKey;
            }
        }

        private readonly struct OrdinaryEdgeKey : IEquatable<OrdinaryEdgeKey> {
            private readonly int _parentIndex;

            private readonly int _childIndex;

            private readonly string _parentKey;

            private readonly string _childKey;

            public OrdinaryEdgeKey(int parentIndex, int childIndex, string parentKey, string childKey) {
                _parentIndex = parentIndex;
                _childIndex = childIndex;
                _parentKey = parentKey;
                _childKey = childKey;
            }

            public bool Equals(OrdinaryEdgeKey other) {
                if (_parentIndex == other._parentIndex && _childIndex == other._childIndex && string.Equals(_parentKey, other._parentKey, StringComparison.OrdinalIgnoreCase)) {
                    return string.Equals(_childKey, other._childKey, StringComparison.OrdinalIgnoreCase);
                }
                return false;
            }

            public override bool Equals(object obj) {
                if (obj is OrdinaryEdgeKey other) {
                    return Equals(other);
                }
                return false;
            }

            public override int GetHashCode() {
                return (((((_parentIndex * 397) ^ _childIndex) * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(_parentKey)) * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(_childKey);
            }
        }

        private readonly struct AggregateRelationKey : IEquatable<AggregateRelationKey> {
            private readonly int _parentIndex;

            private readonly int _childIndex;

            private readonly string _parentKey;

            private readonly string _childKey;

            private readonly AggregateBuildOptions _kind;

            private readonly MemberInfo _target;

            private readonly string[] _sourceFields;

            public AggregateRelationKey(int parentIndex, int childIndex, JoinKeys keys, AggregateBuildOptions kind, MemberInfo target, IEnumerable<string> sourceFields) {
                _parentIndex = parentIndex;
                _childIndex = childIndex;
                _parentKey = keys.ParentKey;
                _childKey = keys.ChildKey;
                _kind = kind;
                _target = target;
                _sourceFields = sourceFields.ToArray();
            }

            public bool Equals(AggregateRelationKey other) {
                if (_parentIndex != other._parentIndex || _childIndex != other._childIndex || _kind != other._kind || !object.Equals(_target, other._target) || !string.Equals(_parentKey, other._parentKey, StringComparison.OrdinalIgnoreCase) || !string.Equals(_childKey, other._childKey, StringComparison.OrdinalIgnoreCase) || _sourceFields.Length != other._sourceFields.Length) {
                    return false;
                }
                for (int i = 0; i < _sourceFields.Length; i++) {
                    if (!string.Equals(_sourceFields[i], other._sourceFields[i], StringComparison.OrdinalIgnoreCase)) {
                        return false;
                    }
                }
                return true;
            }

            public override bool Equals(object obj) {
                if (obj is AggregateRelationKey other) {
                    return Equals(other);
                }
                return false;
            }

            public override int GetHashCode() {
                int parentIndex = _parentIndex;
                parentIndex = (parentIndex * 397) ^ _childIndex;
                parentIndex = (parentIndex * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(_parentKey);
                parentIndex = (parentIndex * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(_childKey);
                parentIndex = (parentIndex * 397) ^ (int)_kind;
                parentIndex = (parentIndex * 397) ^ (_target?.GetHashCode() ?? 0);
                for (int i = 0; i < _sourceFields.Length; i++) {
                    parentIndex = (parentIndex * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(_sourceFields[i]);
                }
                return parentIndex;
            }
        }

        private readonly Type _rootType;

        private readonly AggregateJoinShape _shape;

        private readonly DefinitiveAliasAllocator _aliases = new DefinitiveAliasAllocator();

        private readonly JoinDefinition _definition = new JoinDefinition();

        private readonly Dictionary<string, JoiningTable> _tablesByAlias = new Dictionary<string, JoiningTable>(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, int> _tableIndicesByAlias = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly HashSet<OrdinaryEdgeKey> _ordinaryEdges = new HashSet<OrdinaryEdgeKey>();

        private readonly Dictionary<OrdinaryEdgeKey, int> _unpromotedForwardRelationIndices = new Dictionary<OrdinaryEdgeKey, int>();

        private readonly HashSet<AggregateRelationKey> _aggregateRelations = new HashSet<AggregateRelationKey>();

        public AutomaticCompilation(Type rootType, AggregateJoinShape shape) {
            _rootType = rootType;
            _shape = shape;
        }

        public DefinitiveJoinPlan Compile() {
            string alias = _aliases.GetAlias("root", _rootType, string.Empty);
            EnsureTable(_rootType, alias, JoinType.LEFT, null, GetFieldNames(_rootType), RootContext());
            _aliases.Bind(new AggregatePath(Array.Empty<string>()), alias);
            Visit(_rootType, alias, new AggregatePath(Array.Empty<string>()), isRoot: true, new List<Type> { _rootType });
            DefinitiveJoinPlan definitiveJoinPlan = DefinitiveJoinPlanCompiler.Compile(_definition, _rootType, _shape);
            return new DefinitiveJoinPlan(definitiveJoinPlan.RootType, definitiveJoinPlan.Shape, definitiveJoinPlan.RootTableIndex, definitiveJoinPlan.Tables, definitiveJoinPlan.Relations, definitiveJoinPlan.Projection, _aliases.SnapshotAliasesByPath(), definitiveJoinPlan.TableIndexByAlias, definitiveJoinPlan.RootOrdering, definitiveJoinPlan.FormatVersion);
        }

        private void Visit(Type currentType, string currentAlias, AggregatePath currentPath, bool isRoot, List<Type> ancestry) {
            List<AggregateMetadata> list = new List<AggregateMetadata>();
            foreach (MemberInfo orderedMember in GetOrderedMembers(currentType)) {
                list.Add(new AggregateMetadata(orderedMember, orderedMember.GetCustomAttribute<AggregateFieldAttribute>(inherit: true), orderedMember.GetCustomAttribute<AggregateFarFieldAttribute>(inherit: true), orderedMember.GetCustomAttribute<AggregateObjectAttribute>(inherit: true), orderedMember.GetCustomAttribute<AggregateListAttribute>(inherit: true)));
            }
            foreach (AggregateMetadata item in list) {
                if (item.Field != null && Applies(item.Field, isRoot, item.Member, currentPath)) {
                    EmitAggregateField(currentType, currentAlias, currentPath, item.Member, item.Field);
                }
            }
            foreach (AggregateMetadata item2 in list) {
                if (item2.FarField != null && Applies(item2.FarField, isRoot, item2.Member, currentPath)) {
                    EmitAggregateFarField(currentType, currentAlias, currentPath, item2.Member, item2.FarField);
                }
            }
            if (_shape == AggregateJoinShape.ScalarAggregatesOnly) {
                return;
            }
            foreach (AggregateMetadata item3 in list) {
                if (item3.Object != null && Applies(item3.Object, isRoot, item3.Member, currentPath)) {
                    EmitAggregateObject(currentType, currentAlias, currentPath, item3.Member, item3.Object, ancestry);
                }
            }
            foreach (AggregateMetadata item4 in list) {
                if (item4.List != null && Applies(item4.List, isRoot, item4.Member, currentPath)) {
                    EmitAggregateList(currentType, currentAlias, currentPath, item4.Member, item4.List, ancestry);
                }
            }
        }

        private void EmitAggregateField(Type currentType, string currentAlias, AggregatePath currentPath, MemberInfo target, AggregateFieldAttribute metadata) {
            ValidateWritableMember(target, Context(currentPath, target));
            Type type = RequireType(metadata.RemoteObjectType, "remote object type", currentPath, target);
            MemberInfo memberInfo = ResolveMember(type, RequireName(metadata.RemoteField, "remote source field", currentPath, target), Context(currentPath, target) + " remote source field");
            JoinKeys keys = ResolveDirectJoinKeys(currentType, type, metadata.ObjectKey, currentPath, target);
            string alias = _aliases.GetAlias(currentAlias, type, metadata.ObjectKey);
            EnsureTable(type, alias, JoinType.LEFT, BuildPredicate(currentAlias, alias, keys), Array.Empty<string>(), Context(currentPath, target));
            EnsureOrdinaryRelation(currentAlias, alias, keys);
            AddAggregateRelation(currentAlias, alias, keys, AggregateBuildOptions.AggregateField, target, new string[1] { memberInfo.Name });
            _aliases.Bind(Append(currentPath, target.Name), alias);
        }

        private void EmitAggregateFarField(Type currentType, string currentAlias, AggregatePath currentPath, MemberInfo target, AggregateFarFieldAttribute metadata) {
            ValidateWritableMember(target, Context(currentPath, target));
            Type type = RequireType(metadata.ImediateType, "immediate object type", currentPath, target);
            Type type2 = RequireType(metadata.FarType, "far object type", currentPath, target);
            JoinKeys keys = ResolveDirectJoinKeys(currentType, type, metadata.ImediateKey, currentPath, target);
            string alias = _aliases.GetAlias(currentAlias, type, metadata.ImediateKey);
            EnsureTable(type, alias, JoinType.LEFT, BuildPredicate(currentAlias, alias, keys), Array.Empty<string>(), Context(currentPath, target));
            EnsureOrdinaryRelation(currentAlias, alias, keys);
            JoinKeys keys2 = ResolveDirectJoinKeys(type, type2, metadata.FarKey, currentPath, target);
            string alias2 = _aliases.GetAlias(alias, type2, metadata.FarKey);
            EnsureTable(type2, alias2, JoinType.LEFT, BuildPredicate(alias, alias2, keys2), Array.Empty<string>(), Context(currentPath, target));
            EnsureOrdinaryRelation(alias, alias2, keys2);
            MemberInfo memberInfo = ResolveMember(type2, RequireName(metadata.FarField, "far source field", currentPath, target), Context(currentPath, target));
            MemberInfo memberInfo2 = ResolveAutomaticIdentifierMember(currentType, Context(currentPath, target));
            MemberInfo memberInfo3 = ResolveAutomaticIdentifierMember(type2, Context(currentPath, target));
            AddAggregateRelation(currentAlias, alias2, new JoinKeys(memberInfo2.Name, memberInfo3.Name), AggregateBuildOptions.AggregateField, target, new string[1] { memberInfo.Name });
            _aliases.Bind(Append(currentPath, target.Name), alias2);
        }

        private void EmitAggregateObject(Type currentType, string currentAlias, AggregatePath currentPath, MemberInfo target, AggregateObjectAttribute metadata, List<Type> ancestry) {
            ValidateWritableMember(target, Context(currentPath, target));
            Type memberType = GetMemberType(target, Context(currentPath, target));
            if (memberType == typeof(string) || typeof(IEnumerable).IsAssignableFrom(memberType)) {
                throw new ArgumentException(Context(currentPath, target) + " AggregateObject target must be a non-collection object type.");
            }
            if (!typeof(IDataObject).IsAssignableFrom(memberType)) {
                throw new ArgumentException(Context(currentPath, target) + " AggregateObject target must implement IDataObject.");
            }
            JoinKeys keys = ResolveDirectJoinKeys(currentType, memberType, metadata.ObjectKey, currentPath, target);
            string alias = _aliases.GetAlias(currentAlias, memberType, metadata.ObjectKey);
            string[] fieldNames = GetFieldNames(memberType);
            EnsureTable(memberType, alias, JoinType.LEFT, BuildPredicate(currentAlias, alias, keys), fieldNames, Context(currentPath, target));
            PromoteOrdinaryForwardToAggregate(currentAlias, alias, keys, AggregateBuildOptions.AggregateObject, target, fieldNames);
            AggregatePath aggregatePath = Append(currentPath, target.Name);
            _aliases.Bind(aggregatePath, alias);
            VisitChild(memberType, alias, aggregatePath, ancestry, target);
        }

        private void EmitAggregateList(Type currentType, string currentAlias, AggregatePath currentPath, MemberInfo target, AggregateListAttribute metadata, List<Type> ancestry) {
            ValidateWritableMember(target, Context(currentPath, target));
            Type type = RequireType(metadata.RemoteObjectType, "list remote object type", currentPath, target);
            if (!IsCollectionOf(GetMemberType(target, Context(currentPath, target)), type)) {
                throw new ArgumentException($"{Context(currentPath, target)} AggregateList target must be a collection of '{type}'.");
            }
            JoinKeys keys = ResolveDirectJoinKeys(currentType, type, metadata.RemoteField, currentPath, target);
            string alias = _aliases.GetAlias(currentAlias, type, metadata.RemoteField);
            string[] fieldNames = GetFieldNames(type);
            EnsureTable(type, alias, JoinType.RIGHT, BuildPredicate(currentAlias, alias, keys), fieldNames, Context(currentPath, target));
            PromoteOrdinaryForwardToAggregate(currentAlias, alias, keys, AggregateBuildOptions.AggregateList, target, fieldNames);
            AggregatePath aggregatePath = Append(currentPath, target.Name);
            _aliases.Bind(aggregatePath, alias);
            VisitChild(type, alias, aggregatePath, ancestry, target);
        }

        private void VisitChild(Type childType, string alias, AggregatePath childPath, List<Type> ancestry, MemberInfo sourceMember) {
            if (ancestry.Contains(childType)) {
                throw new ArgumentException(string.Format("Automatic aggregate cycle for root type '{0}': ancestry '{1}' repeats type '{2}' at aggregate path '{3}' (member '{4}').", _rootType, string.Join(" -> ", ancestry.Select((Type x) => x.Name)), childType, childPath, sourceMember.Name));
            }
            List<Type> ancestry2 = new List<Type>(ancestry) { childType };
            Visit(childType, alias, childPath, isRoot: false, ancestry2);
        }

        private bool Applies(AbstractAggregationAttribute metadata, bool isRoot, MemberInfo member, AggregatePath path) {
            string[] array = (string.IsNullOrWhiteSpace(metadata.Flags) ? "full" : metadata.Flags).Split(',');
            bool result = false;
            bool result2 = false;
            bool flag = false;
            bool flag2 = false;
            for (int i = 0; i < array.Length; i++) {
                string text = array[i].Trim().ToLowerInvariant();
                switch (text) {
                case "root":
                    result = true;
                    flag2 = true;
                    continue;
                case "child":
                    result2 = true;
                    flag2 = true;
                    continue;
                case "full":
                case "default":
                    flag = true;
                    flag2 = true;
                    continue;
                }
                if (text.Length > 0) {
                    throw new ArgumentException(Context(path, member) + " has unsupported aggregation flag '" + array[i] + "'.");
                }
            }
            if (!(!flag2 || flag)) {
                if (!isRoot) {
                    return result2;
                }
                return result;
            }
            return true;
        }

        private void EnsureTable(Type type, string alias, JoinType kind, string predicate, IEnumerable<string> initialFields, string context = null) {
            if (_tablesByAlias.TryGetValue(alias, out var value)) {
                if (value.ValueObject != type) {
                    throw new ArgumentException(string.Format("{0} alias '{1}' maps to both '{2}' and '{3}'.", context ?? "Automatic aggregate", alias, value.ValueObject, type));
                }
                if (value.Type != kind) {
                    throw new ArgumentException(string.Format("{0} alias '{1}' was requested with conflicting join kinds '{2}' and '{3}'.", context ?? "Automatic aggregate", alias, value.Type, kind));
                }
                if (!string.Equals(value.Args, predicate, StringComparison.Ordinal)) {
                    throw new ArgumentException((context ?? "Automatic aggregate") + " alias '" + alias + "' was requested with conflicting join predicates '" + value.Args + "' and '" + predicate + "'.");
                } {
                    foreach (string field in initialFields) {
                        if (!value.Columns.Any((string existingField) => string.Equals(existingField, field, StringComparison.OrdinalIgnoreCase))) {
                            value.Columns.Add(field);
                        }
                    }
                    return;
                }
            }
            ResolveAutomaticIdentifierMember(type, context ?? RootContext());
            JoiningTable joiningTable = new JoiningTable {
                ValueObject = type,
                TableName = type.Name,
                Alias = alias,
                Prefix = alias,
                Type = kind,
                Args = predicate,
                Columns = new List<string>(initialFields)
            };
            _tablesByAlias.Add(alias, joiningTable);
            _tableIndicesByAlias.Add(alias, _definition.Joins.Count);
            _definition.Joins.Add(joiningTable);
        }

        private void EnsureOrdinaryRelation(string parentAlias, string childAlias, JoinKeys keys) {
            int num = TableIndex(parentAlias);
            int num2 = TableIndex(childAlias);
            OrdinaryEdgeKey ordinaryEdgeKey = new OrdinaryEdgeKey(num, num2, keys.ParentKey, keys.ChildKey);
            if (_ordinaryEdges.Add(ordinaryEdgeKey)) {
                _unpromotedForwardRelationIndices.Add(ordinaryEdgeKey, _definition.Relations.Count);
                _definition.Relations.Add(NewRelation(num, num2, keys, AggregateBuildOptions.None, null, Array.Empty<string>()));
                _definition.Relations.Add(NewRelation(num2, num, new JoinKeys(keys.ChildKey, keys.ParentKey), AggregateBuildOptions.None, null, Array.Empty<string>()));
            }
        }

        private void AddAggregateRelation(string parentAlias, string childAlias, JoinKeys keys, AggregateBuildOptions kind, MemberInfo target, IEnumerable<string> fields) {
            int parentIndex = TableIndex(parentAlias);
            int childIndex = TableIndex(childAlias);
            string[] array = fields.ToArray();
            AggregateRelationKey item = new AggregateRelationKey(parentIndex, childIndex, keys, kind, target, array);
            if (_aggregateRelations.Add(item)) {
                _definition.Relations.Add(NewRelation(parentIndex, childIndex, keys, kind, target, array));
            }
        }

        private void PromoteOrdinaryForwardToAggregate(string parentAlias, string childAlias, JoinKeys keys, AggregateBuildOptions kind, MemberInfo target, IEnumerable<string> fields) {
            int parentIndex = TableIndex(parentAlias);
            int childIndex = TableIndex(childAlias);
            EnsureOrdinaryRelation(parentAlias, childAlias, keys);
            string[] array = fields.ToArray();
            AggregateRelationKey item = new AggregateRelationKey(parentIndex, childIndex, keys, kind, target, array);
            if (_aggregateRelations.Add(item)) {
                OrdinaryEdgeKey key = new OrdinaryEdgeKey(parentIndex, childIndex, keys.ParentKey, keys.ChildKey);
                Relation relation = NewRelation(parentIndex, childIndex, keys, kind, target, array);
                if (_unpromotedForwardRelationIndices.TryGetValue(key, out var value)) {
                    _definition.Relations[value] = relation;
                    _unpromotedForwardRelationIndices.Remove(key);
                }
                else {
                    _definition.Relations.Add(relation);
                }
            }
        }

        private static Relation NewRelation(int parentIndex, int childIndex, JoinKeys keys, AggregateBuildOptions kind, MemberInfo target, IEnumerable<string> fields) {
            return new Relation {
                ParentIndex = parentIndex,
                ChildIndex = childIndex,
                ParentKey = keys.ParentKey,
                ChildKey = keys.ChildKey,
                AggregateBuildOption = kind,
                NewName = target?.Name,
                Fields = new List<string>(fields)
            };
        }

        private int TableIndex(string alias) {
            if (_tableIndicesByAlias.TryGetValue(alias, out var value)) {
                return value;
            }
            throw new InvalidOperationException("Alias '" + alias + "' was not added to the automatic join graph.");
        }

        private JoinKeys ResolveDirectJoinKeys(Type parentType, Type childType, string objectKey, AggregatePath path, MemberInfo member) {
            string name = RequireName(objectKey, "object key", path, member);
            MemberInfo memberInfo = ResolveOptionalMember(parentType, name);
            if (memberInfo != null) {
                ResolveMember(parentType, name, Context(path, member));
                return new JoinKeys(memberInfo.Name, ResolveAutomaticIdentifierMember(childType, Context(path, member)).Name);
            }
            MemberInfo memberInfo2 = ResolveMember(childType, name, Context(path, member));
            return new JoinKeys(ResolveAutomaticIdentifierMember(parentType, Context(path, member)).Name, memberInfo2.Name);
        }

        private static string BuildPredicate(string parentAlias, string childAlias, JoinKeys keys) {
            return parentAlias + "." + keys.ParentKey + "=" + childAlias + "." + keys.ChildKey;
        }

        private static string[] GetFieldNames(Type type) {
            return (from x in GetOrderedMembers(type)
                where x.GetCustomAttribute<FieldAttribute>(inherit: true) != null
                select x.Name).ToArray();
        }

        internal static IEnumerable<MemberInfo> GetOrderedMembers(Type type) {
            Stack<Type> stack = new Stack<Type>();
            Type type2 = type;
            while (type2 != null && type2 != typeof(object)) {
                stack.Push(type2);
                type2 = type2.BaseType;
            }
            List<MemberInfo> list = new List<MemberInfo>();
            Dictionary<string, int> dictionary = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            while (stack.Count > 0) {
                foreach (MemberInfo item in from x in stack.Pop().GetMembers(BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public)
                    where x is PropertyInfo || x is FieldInfo
                    orderby x.MetadataToken
                    select x) {
                    if (dictionary.TryGetValue(item.Name, out var value)) {
                        if (list[value] is PropertyInfo != item is PropertyInfo) {
                            throw new ArgumentException($"Entity type '{type}' has incompatible public field/property members named '{item.Name}' in its inheritance hierarchy.", "type");
                        }
                        list[value] = item;
                    }
                    else {
                        dictionary.Add(item.Name, list.Count);
                        list.Add(item);
                    }
                }
            }
            return list;
        }

        private static AggregatePath Append(AggregatePath path, string segment) {
            return new AggregatePath(new List<string>(path.Segments) { segment });
        }

        private Type RequireType(Type value, string role, AggregatePath path, MemberInfo member) {
            if (value == null) {
                throw new ArgumentException(Context(path, member) + " requires a " + role + ".");
            }
            return value;
        }

        private string RequireName(string value, string role, AggregatePath path, MemberInfo member) {
            if (string.IsNullOrWhiteSpace(value)) {
                throw new ArgumentException(Context(path, member) + " requires a non-empty " + role + ".");
            }
            return value;
        }

        private static Type GetMemberType(MemberInfo member, string context) {
            ValidateDataMember(member, context);
            return DefinitivePlanValidation.GetMemberType(member, "member");
        }

        private static void ValidateWritableMember(MemberInfo member, string context) {
            if ((member is PropertyInfo propertyInfo && propertyInfo.SetMethod != null && propertyInfo.GetIndexParameters().Length == 0) || member is FieldInfo { IsInitOnly: false, IsLiteral: false }) {
                return;
            }
            throw new ArgumentException(context + " must target a writable, non-indexed property or mutable field.");
        }

        private static bool IsCollectionOf(Type targetType, Type elementType) {
            if (targetType == typeof(string) || !typeof(IEnumerable).IsAssignableFrom(targetType)) {
                return false;
            }
            if (targetType.IsArray) {
                return targetType.GetElementType() == elementType;
            }
            return targetType.GetInterfaces().Concat<Type>(new Type[1] { targetType }).Any((Type x) => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IEnumerable<>) && x.GetGenericArguments()[0] == elementType);
        }

        private MemberInfo ResolveAutomaticIdentifierMember(Type entityType, string context) {
            try {
                return ResolveIdentifierMember(entityType, entityType.Name);
            }
            catch (ArgumentException ex) {
                throw new ArgumentException($"{context} requires a resolvable identifier member on entity type '{entityType}': {ex.Message}", "entityType", ex);
            }
        }

        private string RootContext() {
            return "Automatic aggregate for root type '" + _rootType.Name + "' at path '<root>'";
        }

        private string Context(AggregatePath path, MemberInfo member) {
            return "Automatic aggregate for root type '" + _rootType.Name + "' at path '" + ((path.Segments.Length == 0) ? member.Name : (path.ToString() + "." + member.Name)) + "'";
        }
    }

    private sealed class CompiledTable {
        public Type EntityType { get; }

        public string TableName { get; }

        public string Alias { get; }

        public string Prefix { get; }

        public JoinType JoinKind { get; }

        public string JoinPredicate { get; }

        public MemberInfo Identifier { get; }

        public List<CompiledColumn> Columns { get; }

        public CompiledTable(Type entityType, string tableName, string alias, string prefix, JoinType joinKind, string joinPredicate, MemberInfo identifier, List<CompiledColumn> columns) {
            EntityType = entityType;
            TableName = tableName;
            Alias = alias;
            Prefix = prefix;
            JoinKind = joinKind;
            JoinPredicate = joinPredicate;
            Identifier = identifier;
            Columns = columns;
        }
    }

    private sealed class CompiledColumn {
        public string Name { get; }

        public MemberInfo Member { get; }

        public CompiledColumn(string name, MemberInfo member) {
            Name = name;
            Member = member;
        }
    }

    public const int CurrentFormatVersion = 1;

    private static readonly Regex JoinPredicatePattern = new Regex("^\\s*(?<leftPrefix>\\w+)\\s*\\.\\s*(?<leftMember>\\w+)\\s*=\\s*(?<rightPrefix>\\w+)\\s*\\.\\s*(?<rightMember>\\w+)\\s*$", RegexOptions.CultureInvariant);

    public static DefinitiveJoinPlan Compile(JoinDefinition definition, Type rootType, AggregateJoinShape shape) {
        if (definition == null) {
            throw new ArgumentNullException("definition");
        }
        if (rootType == null) {
            throw new ArgumentNullException("rootType");
        }
        if (!Enum.IsDefined(typeof(AggregateJoinShape), shape)) {
            throw new ArgumentOutOfRangeException("shape", shape, "Aggregate join shape must be a defined value.");
        }
        if (definition.Joins == null) {
            throw new ArgumentException("Join definition must provide a joins collection.", "definition");
        }
        if (definition.Relations == null) {
            throw new ArgumentException("Join definition must provide a relations collection.", "definition");
        }
        if (definition.Joins.Count == 0) {
            throw new ArgumentException("Join definition must contain a root table at index 0.", "definition");
        }
        List<CompiledTable> list = new List<CompiledTable>(definition.Joins.Count);
        List<KeyValuePair<string, int>> list2 = new List<KeyValuePair<string, int>>(definition.Joins.Count);
        List<KeyValuePair<AggregatePath, string>> list3 = new List<KeyValuePair<AggregatePath, string>>(definition.Joins.Count);
        HashSet<string> hashSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        HashSet<string> hashSet2 = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < definition.Joins.Count; i++) {
            JoiningTable joiningTable = definition.Joins[i] ?? throw new ArgumentException($"Join table at index {i} cannot be null.", "definition");
            Type type = joiningTable.ValueObject ?? throw new ArgumentException($"Join table at index {i} must specify an entity type.", "definition");
            if (!Enum.IsDefined(typeof(JoinType), joiningTable.Type)) {
                throw new ArgumentOutOfRangeException("definition", joiningTable.Type, $"Join table at index {i} has an undefined join type.");
            }
            if (i == 0 && type != rootType) {
                throw new ArgumentException($"Root table at index 0 has entity type '{type}', which must exactly match root type '{rootType}'.", "rootType");
            }
            string text = RequireText(joiningTable.Alias, $"Join table at index {i} alias");
            string text2 = RequireText(joiningTable.Prefix, "Join table '" + text + "' prefix");
            string tableName = RequireText(joiningTable.TableName, "Join table '" + text + "' table name");
            if (!hashSet.Add(text)) {
                throw new ArgumentException("Join table alias '" + text + "' is duplicated ignoring SQL identifier case.", "definition");
            }
            if (!hashSet2.Add(text2)) {
                throw new ArgumentException("Join table SQL prefix '" + text2 + "' is duplicated ignoring SQL identifier case and would produce a result alias collision.", "definition");
            }
            if (joiningTable.Columns == null) {
                throw new ArgumentException("Join table '" + text + "' must provide a columns collection.", "definition");
            }
            MemberInfo memberInfo = ResolveIdentifierMember(type, text);
            List<CompiledColumn> list4 = new List<CompiledColumn>(joiningTable.Columns.Count + 1);
            HashSet<string> hashSet3 = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int j = 0; j < joiningTable.Columns.Count; j++) {
                string text3 = RequireText(joiningTable.Columns[j], $"Join table '{text}' column at index {j}");
                MemberInfo memberInfo2 = ResolveMember(type, text3, "Join table '" + text + "' column '" + text3 + "'");
                if (!hashSet3.Add(memberInfo2.Name)) {
                    throw new ArgumentException("Join table '" + text + "' projects column '" + text3 + "' more than once.", "definition");
                }
                list4.Add(new CompiledColumn(memberInfo2.Name, memberInfo2));
            }
            if (hashSet3.Add(memberInfo.Name)) {
                list4.Add(new CompiledColumn(memberInfo.Name, memberInfo));
            }
            list.Add(new CompiledTable(type, tableName, text, text2, joiningTable.Type, joiningTable.Args, memberInfo, list4));
            list2.Add(new KeyValuePair<string, int>(text, i));
            list3.Add(new KeyValuePair<AggregatePath, string>(new AggregatePath(new string[1] { text }), text));
        }
        List<DefinitiveJoinRelation> relations = CompileRelations(definition.Relations, list);
        bool[] rootReachable = ValidateRelationTopology(relations, list.Count);
        ValidateJoinPredicates(list, rootReachable);
        List<DefinitiveProjectionColumn> list5 = new List<DefinitiveProjectionColumn>();
        List<DefinitiveJoinTable> list6 = new List<DefinitiveJoinTable>(list.Count);
        for (int k = 0; k < list.Count; k++) {
            CompiledTable compiledTable = list[k];
            int num = -1;
            string resultAlias = null;
            for (int l = 0; l < compiledTable.Columns.Count; l++) {
                CompiledColumn compiledColumn = compiledTable.Columns[l];
                int count = list5.Count;
                string text4 = compiledTable.Prefix + "_" + compiledColumn.Name;
                list5.Add(new DefinitiveProjectionColumn(count, k, compiledColumn.Name, text4, compiledColumn.Member));
                if (string.Equals(compiledColumn.Name, compiledTable.Identifier.Name, StringComparison.OrdinalIgnoreCase)) {
                    num = count;
                    resultAlias = text4;
                }
            }
            if (num < 0) {
                throw new ArgumentException("Join table '" + compiledTable.Alias + "' did not produce an identifier projection.", "definition");
            }
            list6.Add(new DefinitiveJoinTable(compiledTable.EntityType, compiledTable.TableName, compiledTable.Alias, compiledTable.Prefix, compiledTable.JoinKind, compiledTable.JoinPredicate, new DefinitiveIdentifier(compiledTable.Identifier, compiledTable.Identifier.Name, num, resultAlias), compiledTable.Columns.Select((CompiledColumn x) => x.Name)));
        }
        DefinitiveIdentifier identifier = list6[0].Identifier;
        return new DefinitiveJoinPlan(rootType, shape, 0, list6, relations, list5, list3, list2, new RootOrderingRequirement(0, identifier.ColumnName, identifier.ProjectionOrdinal, identifier.ResultAlias), 1);
    }

    public static DefinitiveJoinPlan Compile(IJoinBuilder builder, Type rootType, AggregateJoinShape shape) {
        if (builder == null) {
            throw new ArgumentNullException("builder");
        }
#pragma warning disable CS0618
        return Compile(builder.GetJoin() ?? throw new ArgumentException("Join builder returned a null join definition.", "builder"), rootType, shape);
#pragma warning restore CS0618
    }

    public static DefinitiveJoinPlan Compile(Type rootType, AggregateJoinShape shape) {
        if (rootType == null) {
            throw new ArgumentNullException("rootType");
        }
        if (!Enum.IsDefined(typeof(AggregateJoinShape), shape)) {
            throw new ArgumentOutOfRangeException("shape", shape, "Aggregate join shape must be a defined value.");
        }
        return new AutomaticCompilation(rootType, shape).Compile();
    }

    private static List<DefinitiveJoinRelation> CompileRelations(IList<Relation> sourceRelations, IList<CompiledTable> tables) {
        List<DefinitiveJoinRelation> list = new List<DefinitiveJoinRelation>(sourceRelations.Count);
        for (int i = 0; i < sourceRelations.Count; i++) {
            Relation relation = sourceRelations[i] ?? throw new ArgumentException($"Relation at index {i} cannot be null.", "sourceRelations");
            if (!Enum.IsDefined(typeof(AggregateBuildOptions), relation.AggregateBuildOption)) {
                throw new ArgumentOutOfRangeException("sourceRelations", relation.AggregateBuildOption, $"Relation at index {i} has an undefined aggregate build option.");
            }
            ValidateRelationIndex(relation.ParentIndex, tables.Count, "parent", i);
            ValidateRelationIndex(relation.ChildIndex, tables.Count, "child", i);
            string parentKey = relation.ParentKey;
            string childKey = relation.ChildKey;
            if (string.IsNullOrWhiteSpace(parentKey) || string.IsNullOrWhiteSpace(childKey)) {
                Relation relation2 = RecoverRelationKeys(sourceRelations, relation, i);
                parentKey = relation2.ParentKey;
                childKey = relation2.ChildKey;
            }
            if (string.IsNullOrWhiteSpace(parentKey) || string.IsNullOrWhiteSpace(childKey)) {
                throw new ArgumentException($"Relation at index {i} requires non-empty parent and child keys.", "sourceRelations");
            }
            if (relation.Fields == null) {
                throw new ArgumentException($"Relation at index {i} must provide a fields collection.", "sourceRelations");
            }
            CompiledTable compiledTable = tables[relation.ParentIndex];
            CompiledTable compiledTable2 = tables[relation.ChildIndex];
            MemberInfo memberInfo = ResolveMember(compiledTable.EntityType, parentKey, $"Relation at index {i} parent key '{parentKey}'");
            MemberInfo memberInfo2 = ResolveMember(compiledTable2.EntityType, childKey, $"Relation at index {i} child key '{childKey}'");
            AddRequiredColumn(compiledTable, memberInfo);
            AddRequiredColumn(compiledTable2, memberInfo2);
            List<string> list2 = new List<string>(relation.Fields.Count);
            for (int j = 0; j < relation.Fields.Count; j++) {
                string text = RequireText(relation.Fields[j], $"Relation at index {i} source field at index {j}");
                MemberInfo memberInfo3 = ResolveMember(compiledTable2.EntityType, text, $"Relation at index {i} source field '{text}'");
                list2.Add(memberInfo3.Name);
                AddRequiredColumn(compiledTable2, memberInfo3);
            }
            MemberInfo targetMember = null;
            if (relation.AggregateBuildOption != 0) {
                string text2 = RequireText(relation.NewName, $"Aggregate relation at index {i} target name");
                targetMember = ResolveWritableMember(compiledTable.EntityType, text2, $"Aggregate relation at index {i} target '{text2}'");
            }
            list.Add(new DefinitiveJoinRelation(relation.ParentIndex, relation.ChildIndex, memberInfo.Name, memberInfo2.Name, relation.AggregateBuildOption, targetMember, list2));
        }
        return list;
    }

    private static void AddRequiredColumn(CompiledTable table, MemberInfo member) {
        if (!table.Columns.Any((CompiledColumn x) => string.Equals(x.Name, member.Name, StringComparison.OrdinalIgnoreCase))) {
            table.Columns.Add(new CompiledColumn(member.Name, member));
        }
    }

    private static bool[] ValidateRelationTopology(IList<DefinitiveJoinRelation> relations, int tableCount) {
        bool[] rootReachable = new bool[tableCount];
        rootReachable[0] = true;
        int tableIndex;
        for (tableIndex = 1; tableIndex < tableCount; tableIndex++) {
            if (!relations.Any((DefinitiveJoinRelation relation) => (relation.ParentTableIndex == tableIndex && relation.ChildTableIndex < tableIndex && rootReachable[relation.ChildTableIndex]) || (relation.ChildTableIndex == tableIndex && relation.ParentTableIndex < tableIndex && rootReachable[relation.ParentTableIndex]))) {
                throw new ArgumentException($"Relation topology does not connect non-root table at index {tableIndex} to an earlier root-reachable table.", "relations");
            }
            rootReachable[tableIndex] = true;
        }
        return rootReachable;
    }

    private static void ValidateJoinPredicates(IList<CompiledTable> tables, bool[] rootReachable) {
        Dictionary<string, int> dictionary = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < tables.Count; i++) {
            dictionary.Add(tables[i].Prefix, i);
        }
        for (int j = 1; j < tables.Count; j++) {
            CompiledTable compiledTable = tables[j];
            if (string.IsNullOrWhiteSpace(compiledTable.JoinPredicate)) {
                if (compiledTable.JoinKind == JoinType.CROSS) {
                    throw new ArgumentException("Join table '" + compiledTable.Alias + "' uses unsupported predicate-less CROSS join.", "tables");
                }
                throw new ArgumentException("Join table '" + compiledTable.Alias + "' requires a non-empty join predicate.", "tables");
            }
            Match match = JoinPredicatePattern.Match(compiledTable.JoinPredicate);
            if (!match.Success) {
                throw new ArgumentException("Join table '" + compiledTable.Alias + "' predicate '" + compiledTable.JoinPredicate + "' must use the equality shape 'prefix.member = prefix.member'.", "tables");
            }
            int num = ResolvePredicatePrefix(dictionary, match.Groups["leftPrefix"].Value, compiledTable.Alias);
            int num2 = ResolvePredicatePrefix(dictionary, match.Groups["rightPrefix"].Value, compiledTable.Alias);
            ResolveMember(tables[num].EntityType, match.Groups["leftMember"].Value, "Join table '" + compiledTable.Alias + "' predicate member '" + match.Groups["leftMember"].Value + "'");
            ResolveMember(tables[num2].EntityType, match.Groups["rightMember"].Value, "Join table '" + compiledTable.Alias + "' predicate member '" + match.Groups["rightMember"].Value + "'");
            bool num3 = num == j;
            bool flag = num2 == j;
            if (num3 == flag) {
                throw new ArgumentException("Join table '" + compiledTable.Alias + "' predicate must reference the current table prefix exactly once.", "tables");
            }
            int num4 = (num3 ? num2 : num);
            if (num4 >= j || !rootReachable[num4]) {
                throw new ArgumentException("Join table '" + compiledTable.Alias + "' predicate must connect the current table to an earlier root-reachable table.", "tables");
            }
        }
    }

    private static int ResolvePredicatePrefix(IDictionary<string, int> prefixIndices, string prefix, string currentAlias) {
        if (!prefixIndices.TryGetValue(prefix, out var value)) {
            throw new ArgumentException("Join table '" + currentAlias + "' predicate references unknown prefix '" + prefix + "'.", "prefix");
        }
        return value;
    }

    private static Relation RecoverRelationKeys(IList<Relation> relations, Relation aggregateRelation, int aggregateIndex) {
        if (aggregateRelation.AggregateBuildOption == AggregateBuildOptions.None) {
            throw new ArgumentException($"Ordinary relation at index {aggregateIndex} requires non-empty parent and child keys.", "relations");
        }
        List<Relation> list = new List<Relation>();
        for (int i = 0; i < relations.Count; i++) {
            Relation relation = relations[i];
            if (relation != null && relation.AggregateBuildOption == AggregateBuildOptions.None && relation.ParentIndex == aggregateRelation.ParentIndex && relation.ChildIndex == aggregateRelation.ChildIndex && !string.IsNullOrWhiteSpace(relation.ParentKey) && !string.IsNullOrWhiteSpace(relation.ChildKey)) {
                list.Add(relation);
            }
        }
        if (list.Count != 1) {
            throw new ArgumentException($"Aggregate relation at index {aggregateIndex} has missing keys and ambiguously matches {list.Count} ordinary relations; exactly one matching relation is required to recover keys.", "relations");
        }
        return list[0];
    }

    private static void ValidateRelationIndex(int index, int tableCount, string role, int relationIndex) {
        if (index < 0 || index >= tableCount) {
            throw new ArgumentException($"Relation at index {relationIndex} has {role} table index {index}, which is outside the table range.", "index");
        }
    }

    private static MemberInfo ResolveIdentifierMember(Type entityType, string alias) {
        MemberInfo memberInfo = (typeof(ILegacyDataObject).IsAssignableFrom(entityType) ? Fi.Tech.GetRidColumn(entityType) : Fi.Tech.GetIdColumn(entityType));
        if (memberInfo != null) {
            ValidateDataMember(memberInfo, "Identifier for join table '" + alias + "'");
            return memberInfo;
        }
        string text = (typeof(ILegacyDataObject).IsAssignableFrom(entityType) ? "RID" : "Id");
        MemberInfo memberInfo2 = ResolveOptionalMember(entityType, text);
        if (memberInfo2 == null) {
            throw new ArgumentException($"Join table '{alias}' entity type '{entityType}' has no resolvable {text} identifier member.", "entityType");
        }
        ValidateDataMember(memberInfo2, "Identifier for join table '" + alias + "'");
        return memberInfo2;
    }

    private static MemberInfo ResolveMember(Type entityType, string name, string context) {
        MemberInfo memberInfo = ResolveOptionalMember(entityType, name);
        if (memberInfo == null) {
            throw new ArgumentException($"{context} does not resolve to a field or property on entity type '{entityType}'.", "name");
        }
        ValidateDataMember(memberInfo, context);
        return memberInfo;
    }

    private static MemberInfo ResolveWritableMember(Type entityType, string name, string context) {
        MemberInfo memberInfo = ResolveMember(entityType, name, context);
        if (memberInfo is PropertyInfo propertyInfo && propertyInfo.SetMethod != null && propertyInfo.GetIndexParameters().Length == 0) {
            return memberInfo;
        }
        if (memberInfo is FieldInfo { IsInitOnly: false, IsLiteral: false }) {
            return memberInfo;
        }
        throw new ArgumentException(context + " must resolve to a writable, non-indexed property or mutable field.", "name");
    }

    private static MemberInfo ResolveOptionalMember(Type entityType, string name) {
        MemberInfo[] array = (from x in AutomaticCompilation.GetOrderedMembers(entityType)
            where string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase)
            select x).ToArray();
        if (array.Length > 1) {
            throw new ArgumentException($"Member name '{name}' is ambiguous on entity type '{entityType}'.", "name");
        }
        return array.SingleOrDefault();
    }

    private static void ValidateDataMember(MemberInfo member, string context) {
        if (member is PropertyInfo propertyInfo) {
            if (propertyInfo.GetIndexParameters().Length == 0) {
                return;
            }
        }
        else if (member is FieldInfo) {
            return;
        }
        throw new ArgumentException(context + " must resolve to a non-indexed field or property.", "member");
    }

    private static string RequireText(string value, string context) {
        if (string.IsNullOrWhiteSpace(value)) {
            throw new ArgumentException(context + " must be non-empty.");
        }
        return value;
    }
}
}
