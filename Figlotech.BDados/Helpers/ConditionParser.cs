using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Exceptions;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Data;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Figlotech.BDados.Helpers {
    public sealed class ConditionParser {
        private Type rootType;
        private readonly AggregateJoinShape _shape;
        private readonly DefinitiveJoinPlan _configuredPlan;
        private readonly DefinitiveAliasResolver _configuredResolver;
        private readonly PrefixMaker _prefixMaker;
        private DefinitiveAliasResolver _resolver;
        private readonly object _parseSync = new object();

        // Static caches for performance optimization. The compiled expression cache uses weak
        // keys so that expression trees are not kept alive indefinitely by the parser.
        private static readonly ConditionalWeakTable<Expression, Func<object>> _compiledExpressionCache = new ConditionalWeakTable<Expression, Func<object>>();
        private static readonly ConcurrentDictionary<(MemberInfo member, Type attributeType), object> _attributeCache = new ConcurrentDictionary<(MemberInfo member, Type attributeType), object>();
        private static readonly ConcurrentDictionary<Type, MemberInfo[]> _attributedMembersCache = new ConcurrentDictionary<Type, MemberInfo[]>();
        private static readonly ConcurrentDictionary<Type, MemberInfo[]> _allMembersCache = new ConcurrentDictionary<Type, MemberInfo[]>();
        private static readonly ConcurrentDictionary<(string methodName, int paramCount), MethodInfo> _qhMethodCache = new ConcurrentDictionary<(string, int), MethodInfo>();
        private static readonly object _missingAttribute = new object();

        public ConditionParser() {
            _shape = AggregateJoinShape.FullGraph;
            _prefixMaker = new PrefixMaker();
        }
        public ConditionParser(AggregateJoinShape shape) {
            if (!Enum.IsDefined(typeof(AggregateJoinShape), shape)) {
                throw new ArgumentOutOfRangeException(nameof(shape), shape, "Aggregate join shape must be a defined value.");
            }
            _shape = shape;
        }
        public ConditionParser(DefinitiveJoinPlan plan) {
            _configuredPlan = plan ?? throw new ArgumentNullException(nameof(plan));
            _shape = plan.Shape;
        }
        public ConditionParser(DefinitiveAliasResolver resolver) {
            _configuredResolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
            _shape = resolver.Shape;
        }
        public ConditionParser(PrefixMaker prefixMaker) {
            _shape = AggregateJoinShape.FullGraph;
            _prefixMaker = prefixMaker ?? new PrefixMaker();
        }

        // Cached attribute retrieval helper
        private T GetCachedAttribute<T>(MemberInfo member) where T : Attribute {
            if (member == null) {
                return null;
            }

            var key = (member, typeof(T));
            if (_attributeCache.TryGetValue(key, out var cached)) {
                return ReferenceEquals(cached, _missingAttribute) ? null : cached as T;
            }

            var attr = member.GetCustomAttribute<T>(true);
            _attributeCache.TryAdd(key, (object)attr ?? _missingAttribute);
            return attr;
        }

        // Cached attributed members retrieval
        private MemberInfo[] GetCachedAttributedMembers(Type type, string memberName) {
            return _attributedMembersCache.GetOrAdd(type, t => {
                return ReflectionTool.GetAttributedMemberValues<FieldAttribute>(t)
                    .Select(x => x.Member)
                    .ToArray();
            }).Where(m => m.Name == memberName).ToArray();
        }

        // Cached member retrieval
        private MemberInfo GetCachedMember(Type type, string memberName) {
            var members = _allMembersCache.GetOrAdd(type, t => {
                return t.GetMembers(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)
                    .Where(m => m.MemberType == MemberTypes.Property || m.MemberType == MemberTypes.Field)
                    .ToArray();
            });

            return members.FirstOrDefault(m => m.Name == memberName);
        }

        private void SelectResolver(Type type, Expression expression) {
            rootType = type ?? throw new ArgumentNullException(nameof(type));
            if (_configuredResolver != null) {
                if (_configuredResolver.RootType != type) {
                    throw new ArgumentException($"Configured frozen alias resolver root type '{_configuredResolver.RootType}' does not match expression root type '{type}'.", nameof(type));
                }
                _resolver = _configuredResolver;
                return;
            }
            if (_configuredPlan != null) {
                if (_configuredPlan.RootType != type) {
                    throw new ArgumentException($"Configured frozen join plan root type '{_configuredPlan.RootType}' does not match expression root type '{type}'.", nameof(type));
                }
                _resolver = new DefinitiveAliasResolver(_configuredPlan);
                return;
            }
            if (_prefixMaker != null && !HasAggregateReference(expression)) {
                _resolver = null;
                return;
            }
            _resolver = new DefinitiveAliasResolver(AutomaticJoinPlanCache.GetOrAdd(type, _shape));
        }

        private string GetPrefixOfExpression(Expression expression) {
            AggregatePath path = GetAggregatePath(expression);
            if (_resolver != null) {
                return _resolver.Resolve(path);
            }
            if (path.Segments.Length != 0 || _prefixMaker == null) {
                throw new InvalidOperationException("A frozen alias resolver must be selected before parsing an aggregate expression.");
            }
            return GetRootAlias();
        }

        private string GetRootAlias() {
            if (_resolver != null) {
                return _resolver.Resolve(default(AggregatePath));
            }
            if (_prefixMaker != null) {
                return _prefixMaker.GetAliasFor("root", rootType.Name, String.Empty);
            }
            throw new InvalidOperationException("An alias resolver must be selected before parsing an expression.");
        }

        private AggregatePath GetAggregatePath(Expression expression) {
            var members = new System.Collections.Generic.List<MemberInfo>();
            Expression current = StripExpression(expression);
            while (current is MemberExpression member) {
                members.Add(member.Member);
                current = StripExpression(member.Expression);
            }
            if (!(current is ParameterExpression parameter) || parameter.Type != rootType) {
                throw new ArgumentException($"Expression '{expression}' is not rooted at parameter type '{rootType}' and cannot be resolved to a frozen alias.", nameof(expression));
            }
            members.Reverse();
            var segments = new System.Collections.Generic.List<string>();
            for (int i = 0; i < members.Count; i++) {
                MemberInfo member = members[i];
                if (member.GetCustomAttribute<AggregateFieldAttribute>(true) != null
                    || member.GetCustomAttribute<AggregateFarFieldAttribute>(true) != null
                    || member.GetCustomAttribute<AggregateObjectAttribute>(true) != null
                    || member.GetCustomAttribute<AggregateListAttribute>(true) != null) {
                    segments.Add(member.Name);
                }
            }
            return new AggregatePath(segments);
        }

        private sealed class ForcedCollectionContext {
            public ForcedCollectionContext(string alias, AggregatePath semanticBasePath, ParameterExpression parameter) {
                Alias = alias;
                SemanticBasePath = semanticBasePath;
                Parameter = parameter;
            }

            public string Alias { get; }
            public AggregatePath SemanticBasePath { get; }
            public ParameterExpression Parameter { get; }
        }

        private static bool IsAggregateMember(MemberInfo member) {
            return member.GetCustomAttribute<AggregateFieldAttribute>(true) != null
                || member.GetCustomAttribute<AggregateFarFieldAttribute>(true) != null
                || member.GetCustomAttribute<AggregateObjectAttribute>(true) != null
                || member.GetCustomAttribute<AggregateListAttribute>(true) != null;
        }

        private static AggregatePath AppendAggregatePath(AggregatePath basePath, System.Collections.Generic.IEnumerable<string> segments) {
            return new AggregatePath(basePath.Segments.Concat(segments));
        }

        private static bool TryGetLocalAggregatePath(Expression expression, ForcedCollectionContext context, out AggregatePath path) {
            path = default;
            if (context == null) {
                return false;
            }

            var members = new System.Collections.Generic.List<MemberInfo>();
            Expression current = StripExpression(expression);
            while (current is MemberExpression member) {
                members.Add(member.Member);
                current = StripExpression(member.Expression);
            }
            if (!(current is ParameterExpression parameter) || parameter != context.Parameter) {
                return false;
            }

            members.Reverse();
            path = AppendAggregatePath(context.SemanticBasePath, members.Where(IsAggregateMember).Select(member => member.Name));
            return true;
        }

        private bool TryGetForcedPrefix(Expression expression, ForcedCollectionContext context, out string prefix) {
            prefix = null;
            if (!TryGetLocalAggregatePath(expression, context, out AggregatePath path)) {
                return false;
            }
            prefix = path == context.SemanticBasePath ? context.Alias : _resolver.Resolve(path);
            return true;
        }

        private string GetMemberPrefix(Expression expression, string forceAlias, ForcedCollectionContext forcedContext) {
            if (TryGetForcedPrefix(expression, forcedContext, out string forcedPrefix)) {
                return forcedPrefix;
            }
            return forcedContext == null && forceAlias != null
                ? forceAlias
                : GetPrefixOfExpression(expression);
        }

        private ForcedCollectionContext GetCollectionContext(Expression collection, LambdaExpression lambda, ForcedCollectionContext parentContext) {
            if (lambda == null || lambda.Parameters.Count != 1) {
                throw new ArgumentException("Collection predicate must have exactly one parameter.", nameof(lambda));
            }
            if (TryGetLocalAggregatePath(collection, parentContext, out AggregatePath localPath)) {
                return new ForcedCollectionContext(_resolver.Resolve(localPath), localPath, lambda.Parameters[0]);
            }

            AggregatePath path = GetAggregatePath(collection);
            return new ForcedCollectionContext(_resolver.Resolve(path), path, lambda.Parameters[0]);
        }

        private static Expression StripExpression(Expression expression) {
            while (expression is UnaryExpression unary
                && (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked || unary.NodeType == ExpressionType.Quote)) {
                expression = unary.Operand;
            }
            return expression;
        }

        private static string RemoveExactQualifiedPrefix(string text, string qualifiedPrefix) {
            if (String.IsNullOrEmpty(text) || String.IsNullOrEmpty(qualifiedPrefix)) {
                return text;
            }

            var result = new System.Text.StringBuilder(text.Length);
            int position = 0;
            while (position < text.Length) {
                int match = text.IndexOf(qualifiedPrefix, position, StringComparison.OrdinalIgnoreCase);
                if (match < 0) {
                    result.Append(text, position, text.Length - position);
                    break;
                }

                bool isQualifiedToken = match == 0 || !IsSqlIdentifierCharacter(text[match - 1]);
                if (isQualifiedToken) {
                    result.Append(text, position, match - position);
                } else {
                    result.Append(text, position, match - position + qualifiedPrefix.Length);
                }
                position = match + qualifiedPrefix.Length;
            }
            return result.ToString();
        }

        private static bool IsSqlIdentifierCharacter(char value) {
            return Char.IsLetterOrDigit(value) || value == '_';
        }

        private readonly struct RootConditionProjection {
            public RootConditionProjection(Expression expression, bool isExact) {
                Expression = expression;
                IsExact = isExact;
            }

            public Expression Expression { get; }
            public bool IsExact { get; }
        }

        private sealed class AggregateReferenceVisitor : ExpressionVisitor {
            public bool HasAggregateReference { get; private set; }

            protected override Expression VisitMember(MemberExpression node) {
                if (IsAggregateMember(node.Member)) {
                    HasAggregateReference = true;
                    return node;
                }
                return base.VisitMember(node);
            }
        }

        private static bool HasAggregateReference(Expression expression) {
            if (expression == null) {
                return false;
            }
            var visitor = new AggregateReferenceVisitor();
            visitor.Visit(expression);
            return visitor.HasAggregateReference;
        }

        private static bool IsTrue(Expression expression) {
            return expression is ConstantExpression constant
                && constant.Type == typeof(bool)
                && Equals(constant.Value, true);
        }

        private bool IsRootOnlyAtomicCondition(Expression expression) {
            if (HasAggregateReference(expression)) {
                return false;
            }

            if (expression is MethodCallExpression method
                && method.Method.DeclaringType == typeof(Qh)
                && method.Arguments.Count > 1
                && GetValue(method.Arguments[1]) is string column) {
                int separator = column.IndexOf('.');
                if (separator > 0) {
                    string alias = column.Substring(0, separator);
                    string rootAlias = GetRootAlias();
                    return String.Equals(alias, rootAlias, StringComparison.OrdinalIgnoreCase);
                }
            }

            return true;
        }

        private RootConditionProjection ProjectRootCondition(Expression expression) {
            expression = StripExpression(expression);
            if (expression is BinaryExpression binary
                && (binary.NodeType == ExpressionType.AndAlso || binary.NodeType == ExpressionType.And)) {
                RootConditionProjection left = ProjectRootCondition(binary.Left);
                RootConditionProjection right = ProjectRootCondition(binary.Right);
                Expression projected = IsTrue(left.Expression)
                    ? right.Expression
                    : IsTrue(right.Expression)
                        ? left.Expression
                        : Expression.MakeBinary(binary.NodeType, left.Expression, right.Expression);
                return new RootConditionProjection(projected, left.IsExact && right.IsExact);
            }
            if (expression is BinaryExpression orBinary
                && (orBinary.NodeType == ExpressionType.OrElse || orBinary.NodeType == ExpressionType.Or)) {
                RootConditionProjection left = ProjectRootCondition(orBinary.Left);
                RootConditionProjection right = ProjectRootCondition(orBinary.Right);
                if (IsTrue(left.Expression) || IsTrue(right.Expression)) {
                    return new RootConditionProjection(Expression.Constant(true), left.IsExact && right.IsExact);
                }
                return new RootConditionProjection(
                    Expression.MakeBinary(orBinary.NodeType, left.Expression, right.Expression),
                    left.IsExact && right.IsExact);
            }
            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Not) {
                RootConditionProjection operand = ProjectRootCondition(unary.Operand);
                return operand.IsExact
                    ? new RootConditionProjection(Expression.Not(operand.Expression), true)
                    : new RootConditionProjection(Expression.Constant(true), false);
            }

            return IsRootOnlyAtomicCondition(expression)
                ? new RootConditionProjection(expression, true)
                : new RootConditionProjection(Expression.Constant(true), false);
        }

        public QueryBuilder ParseExpression<T>(Conditions<T> c) {
            lock (_parseSync) {
                try {
                    ResetParameterReservation(null);
                    if (c == null) {
                        return new QbFmt("TRUE");
                    }
                    SelectResolver(typeof(T), c.expression);
                    var retv = ParseExpression(c.expression, typeof(T));
                    return retv;
                } catch (Exception x) {
                    throw new BDadosException($"Expression parsing failed for Conditions<T> {c?.expression?.ToString()}; the method/operator must be supported.", x);
                }
            }
        }

        public QueryBuilder ParseExpression<T>(Expression<Func<T, bool>> foofun, bool fullConditions = true, QueryBuilder strBuilder = null) {
            lock (_parseSync) {
                try {
                    ResetParameterReservation(strBuilder);
                    if (foofun == null) {
                        return strBuilder ?? new QbFmt("TRUE");
                    }
                    SelectResolver(typeof(T), foofun.Body);
                    if (!fullConditions) {
                        RootConditionProjection projection = ProjectRootCondition(foofun.Body);
                        if (IsTrue(projection.Expression)) {
                            return strBuilder ?? new QbFmt("TRUE");
                        }
                        QueryBuilder projected = ParseExpression(projection.Expression, typeof(T), null, null, true, forcedContext: null);
                        string rootPrefix = GetRootAlias() + ".";
                        QueryBuilder result = (QueryBuilder)new QueryBuilder().Append(
                            RemoveExactQualifiedPrefix(projected.GetCommandText(), rootPrefix),
                            projected.GetParameters().Select(parameter => parameter.Value).ToArray());
                        if (strBuilder != null) {
                            return (QueryBuilder)strBuilder.Append(result);
                        }
                        return result;
                    }
                    QueryBuilder parsed = ParseExpression(foofun.Body, typeof(T), null, null, fullConditions, forcedContext: null);
                    if (strBuilder != null) {
                        return (QueryBuilder)strBuilder.Append(parsed);
                    }
                    return parsed;
                } catch (Exception x) {
                    throw new BDadosException($"Expression parsing failed for {foofun?.ToString()}", x);
                }
            }
        }

        private bool CanGetValue(Expression member) {
            // Validation-based approach instead of exception-based
            if (member == null) return false;

            // Check if it's a constant expression
            if (member is ConstantExpression) return true;

            // Check if it's a member expression with a valid constant base
            if (member is MemberExpression memEx) {
                return CanGetValue(memEx.Expression);
            }

            // Check if it's a method call expression with constant arguments
            if (member is MethodCallExpression methodEx) {
                // Check if object can be resolved (if not static)
                if (methodEx.Object != null && !CanGetValue(methodEx.Object)) {
                    return false;
                }
                // Check all arguments
                foreach (var arg in methodEx.Arguments) {
                    if (!CanGetValue(arg)) return false;
                }
                return true;
            }

            // Check if it's a unary expression (like Convert)
            if (member is UnaryExpression unaryEx) {
                return CanGetValue(unaryEx.Operand);
            }

            // New expressions can be evaluated
            if (member is NewExpression newEx) {
                foreach (var arg in newEx.Arguments) {
                    if (!CanGetValue(arg)) return false;
                }
                return true;
            }

            return false;
        }

        private object GetValue(Expression member) {
            if (member == null) return null;

            // Try to get from cache first
            if (_compiledExpressionCache.TryGetValue(member, out var cachedGetter)) {
                return cachedGetter();
            }

            try {
                var objectMember = Expression.Convert(member, typeof(object));
                var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                var getter = getterLambda.Compile();

                // Cache the compiled delegate using a weak-key table so expression keys are not
                // retained after the caller stops referencing them.
                try {
                    _compiledExpressionCache.Add(member, getter);
                } catch (ArgumentException) {
                    // Another thread already cached this expression; prefer the cached getter.
                    if (_compiledExpressionCache.TryGetValue(member, out cachedGetter)) {
                        getter = cachedGetter;
                    }
                }

                return getter();
            } catch (Exception ex) {
                throw new BDadosException($"Expression evaluation failed for {member}", ex);
            }
        }

        private bool TryGetQueryIndependentValue(Expression expression, out object value) {
            value = null;
            if (IsCapturableMemberExpression(expression) && TryEvaluateNullIndependent(expression, out value)) {
                return true;
            }
            if (!CanGetValue(expression)) {
                return false;
            }

            value = GetValue(expression);
            return true;
        }

        private bool IsCapturableMemberExpression(Expression expression) {
            if (expression is MemberExpression member) {
                if (member.Expression == null) {
                    return member.Member is PropertyInfo property ? property.GetGetMethod()?.IsStatic == true
                        : member.Member is FieldInfo field && field.IsStatic;
                }
                return IsCapturableMemberExpression(member.Expression);
            }
            if (expression is ConstantExpression) {
                return true;
            }
            if (expression is MethodCallExpression method) {
                if (method.Object != null && !IsCapturableMemberExpression(method.Object)) {
                    return false;
                }
                foreach (var arg in method.Arguments) {
                    if (!IsCapturableMemberExpression(arg)) {
                        return false;
                    }
                }
                return true;
            }
            if (expression is NewExpression newEx) {
                foreach (var arg in newEx.Arguments) {
                    if (!IsCapturableMemberExpression(arg)) {
                        return false;
                    }
                }
                return true;
            }
            if (expression is UnaryExpression unary) {
                return IsCapturableMemberExpression(unary.Operand);
            }
            return false;
        }

        private bool TryEvaluateNullIndependent(Expression expression, out object value) {
            value = null;
            if (expression is ConstantExpression constant) {
                value = constant.Value;
                return true;
            }
            if (expression is MemberExpression member) {
                if (member.Expression == null) {
                    return TryEvaluateNullIndependent(member.Member as PropertyInfo, out value);
                }
                if (TryEvaluateNullIndependent(member.Expression, out object parent)) {
                    if (parent == null) {
                        return true;
                    }
                    if (member.Member is PropertyInfo property) {
                        try {
                            value = property.GetValue(parent);
                            return true;
                        } catch (Exception ex) {
                            throw new BDadosException($"Expression evaluation failed for {expression}", ex);
                        }
                    }
                    if (member.Member is FieldInfo field) {
                        value = field.GetValue(parent);
                        return true;
                    }
                }
                return false;
            }
            if (expression is UnaryExpression unary && (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked)) {
                return TryEvaluateNullIndependent(unary.Operand, out value);
            }
            return false;
        }

        private static bool TryEvaluateNullIndependent(MemberInfo member, out object value) {
            value = null;
            if (member is PropertyInfo property) {
                if (property.GetGetMethod()?.IsStatic != true) {
                    return false;
                }
                try {
                    value = property.GetValue(null);
                    return true;
                } catch (Exception ex) {
                    throw new BDadosException($"Expression evaluation failed for static member {member}", ex);
                }
            }
            if (member is FieldInfo field) {
                if (!field.IsStatic) {
                    return false;
                }
                value = field.GetValue(null);
                return true;
            }
            return false;
        }

        #region Collection Membership (Contains -> IN)

        private static readonly MethodInfo EnumerableContains2Definition = GetEnumerableContains2Definition();

        private static MethodInfo GetEnumerableContains2Definition() {
            return typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Where(m => m.Name == "Contains" && m.GetParameters().Length == 2)
                .Single();
        }

        private bool TryBuildMembershipCondition(MethodCallExpression expr, Type typeOfT, string ForceAlias, bool fullConditions, ForcedCollectionContext forcedContext, bool negated, out QueryBuilder builder) {
            builder = null;
            if (!TryParseMembershipInvocation(expr, out Expression sourceExpression, out Expression testedExpression)) {
                return false;
            }

            if (!TryEvaluateLocalCollectionSource(sourceExpression, out IEnumerable sourceValues)) {
                return false;
            }

            testedExpression = StripNullableValue(testedExpression);
            if (GetMemberFromExpression(testedExpression) == null) {
                throw new NotSupportedException($"Contains membership requires a mapped member expression as the tested value; received '{testedExpression}' instead.");
            }

            if (sourceValues == null) {
                builder = BuildEmptyMembershipCondition(negated);
                return true;
            }

            var nonNullValues = new List<object>();
            int nullCount = 0;
            foreach (object value in sourceValues) {
                if (value == null) {
                    nullCount++;
                } else {
                    nonNullValues.Add(value);
                }
            }

            if (nonNullValues.Count == 0 && nullCount == 0) {
                builder = BuildEmptyMembershipCondition(negated);
                return true;
            }

            MemberInfo member = GetMemberFromExpression(testedExpression);
            bool memberIsNullable = IsNullableType(GetMemberType(member));
            builder = BuildMembershipConditionSql(testedExpression, nonNullValues, nullCount, memberIsNullable, negated, ForceAlias, typeOfT, fullConditions, forcedContext);
            return true;
        }

        private static bool TryParseMembershipInvocation(MethodCallExpression expr, out Expression sourceExpression, out Expression testedExpression) {
            sourceExpression = null;
            testedExpression = null;

            // Static: System.Linq.Enumerable.Contains(source, value)
            if (expr.Method.IsStatic && expr.Method.DeclaringType == typeof(Enumerable) && expr.Method.Name == "Contains") {
                if (expr.Method.GetParameters().Length != 2 || expr.Method.GetGenericMethodDefinition() != EnumerableContains2Definition) {
                    throw new NotSupportedException($"Enumerable.Contains overload with signature ({String.Join(", ", expr.Method.GetParameters().Select(p => p.ParameterType.Name))}) is not supported in condition expressions; only Enumerable.Contains(source, value) is supported.");
                }
                sourceExpression = expr.Arguments[0];
                testedExpression = expr.Arguments[1];
                return true;
            }

            // Instance: standard collection Contains(item)
            if (!expr.Method.IsStatic && expr.Method.Name == "Contains" && expr.Method.GetParameters().Length == 1 && expr.Method.ReturnType == typeof(bool)) {
                Type declaringType = expr.Method.DeclaringType;
                if (declaringType == typeof(string)) {
                    return false;
                }
                if (declaringType != null && declaringType.IsGenericType) {
                    Type genericDef = declaringType.GetGenericTypeDefinition();
                    if (genericDef == typeof(ICollection<>) || genericDef == typeof(IList<>) || genericDef == typeof(ISet<>) || genericDef == typeof(List<>) || genericDef == typeof(HashSet<>)) {
                        sourceExpression = expr.Object;
                        testedExpression = expr.Arguments[0];
                        return true;
                    }
                }
                throw new NotSupportedException($"Method call '{expr.Method.Name}' on type '{declaringType?.Name}' is not a supported collection Contains in condition expressions; only standard collection Contains methods are supported.");
            }

            return false;
        }

        private static Expression StripNullableValue(Expression expression) {
            expression = StripExpression(expression);
            if (expression is MemberExpression member && member.Member.Name == "Value" && member.Member.DeclaringType != null && member.Member.DeclaringType.IsGenericType && member.Member.DeclaringType.GetGenericTypeDefinition() == typeof(Nullable<>)) {
                return member.Expression;
            }
            return expression;
        }

        private static MemberInfo GetMemberFromExpression(Expression expression) {
            expression = StripNullableValue(expression);
            if (expression is MemberExpression member) {
                return member.Member;
            }
            return null;
        }

        private static Type GetMemberType(MemberInfo member) {
            if (member is PropertyInfo property) return property.PropertyType;
            if (member is FieldInfo field) return field.FieldType;
            return null;
        }

        private static bool IsNullableType(Type type) {
            if (type == null) return true;
            if (!type.IsValueType) return true;
            return Nullable.GetUnderlyingType(type) != null;
        }

        private static bool ContainsFreeParameter(Expression expression) {
            var visitor = new FreeParameterVisitor();
            visitor.Visit(expression);
            return visitor.HasFreeParameter;
        }

        private sealed class FreeParameterVisitor : ExpressionVisitor {
            private readonly HashSet<ParameterExpression> _scopedParameters = new HashSet<ParameterExpression>();
            public bool HasFreeParameter { get; private set; }

            protected override Expression VisitLambda<T>(Expression<T> node) {
                foreach (var p in node.Parameters) {
                    _scopedParameters.Add(p);
                }
                var result = base.VisitLambda(node);
                foreach (var p in node.Parameters) {
                    _scopedParameters.Remove(p);
                }
                return result;
            }

            protected override Expression VisitParameter(ParameterExpression node) {
                if (!_scopedParameters.Contains(node)) {
                    HasFreeParameter = true;
                }
                return base.VisitParameter(node);
            }
        }

        private static bool TryEvaluateLocalCollectionSource(Expression sourceExpression, out IEnumerable sourceValues) {
            sourceValues = null;
            if (sourceExpression == null) {
                throw new NotSupportedException("Collection source expression is missing for Contains membership.");
            }

            if (ContainsFreeParameter(sourceExpression)) {
                throw new NotSupportedException("Collection source expression contains a free parameter reference; only local collections and projections are supported in Contains membership.");
            }

            if (sourceExpression.Type == typeof(string)) {
                throw new NotSupportedException("string is not supported as a collection source for Contains membership; use string.Contains for substring matching.");
            }

            try {
                var objectMember = Expression.Convert(sourceExpression, typeof(object));
                var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                var getter = getterLambda.Compile();
                object source = getter();
                if (source == null) {
                    return true;
                }
                if (source is IQueryable) {
                    throw new NotSupportedException("IQueryable collection sources are not supported in Contains membership; materialize the source locally before parsing.");
                }
                if (source is string) {
                    throw new NotSupportedException("string is not supported as a collection source for Contains membership; use string.Contains for substring matching.");
                }
                if (!(source is IEnumerable)) {
                    throw new NotSupportedException($"Collection source expression evaluated to type '{source.GetType().Name}', which is not IEnumerable.");
                }
                sourceValues = (IEnumerable)source;
                return true;
            } catch (Exception ex) when (!(ex is NotSupportedException)) {
                throw new BDadosException($"Collection source expression evaluation failed for Contains membership: {sourceExpression}", ex);
            }
        }

        private QueryBuilder BuildEmptyMembershipCondition(bool negated) {
            return negated ? new QbFmt("1=1") : new QbFmt("1=0");
        }

        private QueryBuilder BuildMembershipConditionSql(Expression testedExpression, List<object> nonNullValues, int nullCount, bool memberIsNullable, bool negated, string ForceAlias, Type typeOfT, bool fullConditions, ForcedCollectionContext forcedContext) {
            if (nonNullValues.Count == 0 && !memberIsNullable) {
                return BuildEmptyMembershipCondition(negated);
            }

            var columnBuilder = ParseExpression(testedExpression, typeOfT, ForceAlias, null, fullConditions, forcedContext);
            string columnSql = columnBuilder.GetCommandText().Trim();

            bool negateNullableNonNullMembership = negated && memberIsNullable && nullCount == 0 && nonNullValues.Count > 0;
            QueryBuilder inner = new QueryBuilder();
            if (nonNullValues.Count == 0) {
                if (nullCount > 0 && memberIsNullable) {
                    inner.Append(columnSql);
                    inner.Append("IS NULL");
                } else {
                    inner.Append(negated ? "1=1" : "1=0");
                }
            } else {
                if (memberIsNullable && (nullCount > 0 || negateNullableNonNullMembership)) {
                    inner.Append("(");
                }
                inner.Append(columnSql);
                inner.Append(negateNullableNonNullMembership ? "NOT IN(" : "IN(");
                for (int i = 0; i < nonNullValues.Count; i++) {
                    if (i > 0) inner.Append(",");
                    inner.Append($"@{GenerateParameterId}", nonNullValues[i]);
                }
                inner.Append(")");
                if (memberIsNullable && (nullCount > 0 || negateNullableNonNullMembership)) {
                    inner.Append("OR");
                    inner.Append(columnSql);
                    inner.Append("IS NULL");
                    inner.Append(")");
                }
            }

            if (negated && !negateNullableNonNullMembership) {
                return Qb.Fmt("NOT(") + inner + Qb.Fmt(")");
            }
            return inner;
        }

        #endregion

        private int idGeneration = -1;
        private readonly HashSet<string> _reservedParameterNames = new HashSet<string>(StringComparer.Ordinal);

        private string GenerateParameterId {
            get {
                while (true) {
                    idGeneration = idGeneration == Int32.MaxValue ? 0 : idGeneration + 1;
                    string parameterName = $"_p{idGeneration}";
                    if (_reservedParameterNames.Add(parameterName)) {
                        return parameterName;
                    }
                }
            }
        }

        private void ResetParameterReservation(QueryBuilder destination) {
            idGeneration = -1;
            _reservedParameterNames.Clear();
            ReserveDestinationParameterIds(destination);
        }

        private void ReserveDestinationParameterIds(QueryBuilder destination) {
            if (destination == null) {
                return;
            }

            foreach (string parameterName in destination.GetParameters().Keys) {
                _reservedParameterNames.Add(parameterName);
            }
        }

        private static string EscapeLikeLiteral(string value) {
            return value?.Replace("!", "!!").Replace("%", "!%").Replace("_", "!_");
        }

        private static bool TryDecomposeAnyInvocation(MethodCallExpression anyExpression, out Expression collection, out LambdaExpression predicate) {
            collection = null;
            predicate = null;

            if (anyExpression.Arguments.Count == 1) {
                collection = anyExpression.Arguments[0];
            } else if (anyExpression.Arguments.Count == 2) {
                collection = anyExpression.Arguments[0];
                predicate = StripExpression(anyExpression.Arguments[1]) as LambdaExpression;
            } else {
                return false;
            }

            if (collection is MethodCallExpression whereCall && whereCall.Method.DeclaringType == typeof(System.Linq.Enumerable) && whereCall.Method.Name == "Where") {
                if (whereCall.Arguments.Count == 2) {
                    predicate = StripExpression(whereCall.Arguments[1]) as LambdaExpression;
                    collection = whereCall.Arguments[0];
                } else {
                    return false;
                }
            }

            return collection != null;
        }

        private QueryBuilder ParseExpression(Expression foofun, Type typeOfT, String ForceAlias = null, QueryBuilder strBuilder = null, bool fullConditions = true, ForcedCollectionContext forcedContext = null) {
            //if(strBuilder == null)
            strBuilder = new QueryBuilder();

            if (foofun is BinaryExpression bexpr) {
                var expr = bexpr;
                bool rightIsNull = TryGetQueryIndependentValue(expr.Right, out object rightValue) && rightValue == null;
                bool leftIsNull = TryGetQueryIndependentValue(expr.Left, out object leftValue) && leftValue == null;

                if ((expr.NodeType == ExpressionType.Equal || expr.NodeType == ExpressionType.NotEqual) &&
                    (rightIsNull || leftIsNull)) {
                    var operand = rightIsNull ? expr.Left : expr.Right;
                    strBuilder.Append("(");
                    strBuilder.Append(ParseExpression(operand, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                    strBuilder.Append(expr.NodeType == ExpressionType.Equal ? "IS NULL" : "IS NOT NULL");
                    strBuilder.Append(")");
                } else if (expr.NodeType == ExpressionType.Equal &&
                            TryGetQueryIndependentValue(expr.Right, out object rightStringValue) &&
                            rightStringValue is string rightString &&
                            (expr.Left is MemberExpression)
                        ) {
                            var member = (expr.Left as MemberExpression).Member;
                            var comparisonType = GetCachedAttribute<QueryComparisonAttribute>(member)?.Type;

                            strBuilder.Append("(");
                            if (comparisonType == DataStringComparisonType.IgnoreCase) {
                                strBuilder.Append("LOWER(");
                                strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                strBuilder.Append($")=LOWER(@{GenerateParameterId})", rightString);
                            } else {
                                strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                var appendFragment = comparisonType switch {
                                    DataStringComparisonType.Containing => $"LIKE CONCAT('%', @{GenerateParameterId}, '%') ESCAPE '!'",
                                    DataStringComparisonType.EndingWith => $"LIKE CONCAT('%', @{GenerateParameterId}) ESCAPE '!'",
                                    DataStringComparisonType.StartingWith => $"LIKE CONCAT(@{GenerateParameterId}, '%') ESCAPE '!'",
                                    DataStringComparisonType.ExactValue => $"=@{GenerateParameterId}",
                                    _ => $"=@{GenerateParameterId}"
                                };
                                bool usesLikeComparison = comparisonType == DataStringComparisonType.Containing
                                    || comparisonType == DataStringComparisonType.EndingWith
                                    || comparisonType == DataStringComparisonType.StartingWith;
                                strBuilder.Append(appendFragment, usesLikeComparison ? EscapeLikeLiteral(rightString) : rightString);
                            }
                            strBuilder.Append(")");
                        } else {
                            string op;
                            switch (expr.NodeType) {
                                case ExpressionType.AndAlso: op = "AND"; break;
                                case ExpressionType.OrElse: op = "OR"; break;
                                case ExpressionType.Equal: op = "="; break;
                                case ExpressionType.NotEqual: op = "!="; break;
                                case ExpressionType.Not: op = "!"; break;
                                case ExpressionType.GreaterThan: op = ">"; break;
                                case ExpressionType.GreaterThanOrEqual: op = ">="; break;
                                case ExpressionType.LessThan: op = "<"; break;
                                case ExpressionType.LessThanOrEqual: op = "<="; break;
                                default:
                                    throw new NotSupportedException($"Binary expression node type '{expr.NodeType}' is not supported in condition expressions.");
                            }

                            strBuilder.Append("(");
                            strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                            strBuilder.Append(op);
                            strBuilder.Append(ParseExpression(expr.Right, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                            strBuilder.Append(")");
                        }
            } else
                if (foofun is MemberExpression) {
                    var expr = foofun as MemberExpression;
                    Expression subexp = expr;
                    while (subexp is MemberExpression sme && sme.Expression != null)
                        subexp = sme.Expression;
                    if (subexp is UnaryExpression uex) {
                        subexp = uex.Operand;
                    }
                    if (subexp is ParameterExpression pex) {
                        // If this member belongs to an AggregateField, then problems problems...
                        var aList = GetCachedAttributedMembers(subexp.Type, expr.Member.Name.Replace("_", ""));
                        if (!fullConditions && (pex.Type != typeOfT || aList.Length == 0)) {
                            return new QbFmt("1");
                        }
                        if (aList.Length > 0) {
                            strBuilder.Append($"{GetMemberPrefix(expr, ForceAlias, forcedContext)}.{expr.Member.Name.Replace("_", "")}");
                        } else {
                            // oh hell.
                            MemberInfo member;
                            if (expr.Expression is ParameterExpression pexpr) {
                                member = GetCachedMember(pexpr.Type, expr.Member.Name);
                            } else if (expr.Expression is MemberExpression memex) {
                                member = GetCachedMember(memex.Type, expr.Member.Name);
                            } else {
                                member = null;
                            }
                            var info = GetCachedAttribute<AggregateFieldAttribute>(member);
                            if (info != null) {
                                var prefix = GetMemberPrefix(expr, ForceAlias, forcedContext);
                                strBuilder.Append($"{prefix}.{info.RemoteField}");
                            } else {
                                var info2 = GetCachedAttribute<AggregateFarFieldAttribute>(expr.Member);
                                if (info2 != null) {
                                    var prefix = GetMemberPrefix(expr, ForceAlias, forcedContext);
                                    strBuilder.Append($"{prefix}.{info2.FarField}");
                                } else {
                                    var mem = GetCachedMember((expr.Expression).Type, expr.Member.Name) ?? expr.Member;
                                    var info3 = GetCachedAttribute<AggregateObjectAttribute>(mem);
                                    var altName = GetCachedAttribute<OverrideColumnNameOnWhere>(mem);
                                    var memberName = altName?.Name ?? member?.Name ?? expr.Member.Name;
                                    if (info3 != null) {
                                        var prefix = GetMemberPrefix(expr, ForceAlias, forcedContext);
                                        strBuilder.Append($"{prefix}.{memberName}");
                                    } else {
                                        var prefix = GetMemberPrefix(expr, ForceAlias, forcedContext);
                                        strBuilder.Append($"{prefix}.{memberName}");
                                    }

                                }
                            }
                        }
                    } else if (subexp is MethodCallExpression) {
                        strBuilder.Append($"{ParseExpression(subexp, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext).GetCommandText()}.{expr.Member.Name}");
                    } else {
                        strBuilder.Append($"@{GenerateParameterId}", GetValue(expr));
                    }
                } else
                    if (foofun is ConstantExpression cexpr) {
                        strBuilder.Append($"@{GenerateParameterId}", cexpr.Value);
                    } else
                        if (foofun is UnaryExpression uexpr) {
                            var expr = uexpr;
                            if (expr.NodeType == ExpressionType.Not) {
                                Expression operand = StripExpression(expr.Operand);
                                if (operand is MethodCallExpression methodCall && TryBuildMembershipCondition(methodCall, typeOfT, ForceAlias, fullConditions, forcedContext, true, out QueryBuilder membershipBuilder)) {
                                    strBuilder.Append(membershipBuilder);
                                } else {
                                    strBuilder.Append("NOT (");
                                    strBuilder.Append(ParseExpression(expr.Operand, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                    strBuilder.Append(")");
                                }
                            }
                            if (expr.NodeType == ExpressionType.Convert) {
                                //strBuilder.Append($"@{GenerateParameterId}", GetValue(expr));
                                strBuilder.Append(ParseExpression(expr.Operand, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                            }
                        } else
                            if (!fullConditions) {
                                return new QbFmt("");
                            } else
                                if (foofun is MethodCallExpression mcexpr) {
                                    var expr = mcexpr;

                                    if (expr.Method.DeclaringType == typeof(StringExtensions)) {
                                        if (expr.Method.Name == nameof(StringExtensions.RegExReplace)) {
                                            var regExReplaceParameters = expr.Method.GetParameters();
                                            if (!expr.Method.IsStatic
                                                || regExReplaceParameters.Length != 3
                                                || regExReplaceParameters[0].ParameterType != typeof(string)
                                                || regExReplaceParameters[1].ParameterType != typeof(string)
                                                || regExReplaceParameters[2].ParameterType != typeof(string)) {
                                                throw new NotSupportedException($"StringExtensions.RegExReplace overload with signature ({String.Join(", ", regExReplaceParameters.Select(parameter => parameter.ParameterType.Name))}) is not supported in condition expressions; only RegExReplace(string, string, string) is supported.");
                                            }
                                            var retv = Qb.Fmt("REGEXP_REPLACE(")
                                                + ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)
                                                + Qb.Fmt(",")
                                                + ParseExpression(expr.Arguments[1], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)
                                                + Qb.Fmt(",")
                                                + ParseExpression(expr.Arguments[2], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)
                                                + Qb.Fmt(")");
                                            return retv;
                                        }
                                    }
                                    if (expr.Method.DeclaringType == typeof(Int32) || expr.Method.DeclaringType == typeof(Int64)) {
                                        if (expr.Method.Name == nameof(Int32.Parse)) {
                                            var parseParameters = expr.Method.GetParameters();
                                            if (!expr.Method.IsStatic
                                                || parseParameters.Length != 1
                                                || parseParameters[0].ParameterType != typeof(string)) {
                                                throw new NotSupportedException($"{expr.Method.DeclaringType.Name}.Parse overload with signature ({String.Join(", ", parseParameters.Select(parameter => parameter.ParameterType.Name))}) is not supported in condition expressions; only Parse(string) is supported.");
                                            }
                                            var retv = Qb.Fmt("CAST(")
                                                + ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)
                                                + Qb.Fmt("AS")
                                                + Qb.Fmt("SIGNED")
                                                + Qb.Fmt(")");
                                            return retv;
                                        }
                                    }

                                    if (expr.Method.DeclaringType == typeof(FiTechCoreExtensions)) {
                                        if (expr.Method.GetParameters().Length == 1) {
                                            return Qb.Fmt($"@{GenerateParameterId}", expr.Method.Invoke(null, new object[] { Fi.Tech }));
                                        }
                                    }

                                    if (expr.Method.DeclaringType == typeof(Qh)) {
                                        var tq = typeof(Qb);
                                        var paramCount = expr.Method.GetParameters().Length - 1;
                                        var cacheKey = (expr.Method.Name, paramCount);

                                        if (!_qhMethodCache.TryGetValue(cacheKey, out var equivalent)) {
                                            equivalent = tq.GetMethods().FirstOrDefault(m => m.Name == expr.Method.Name && m.GetParameters().Length == paramCount);
                                            if (equivalent != null) {
                                                _qhMethodCache.TryAdd(cacheKey, equivalent);
                                            }
                                        }

                                        if (equivalent != null) {
                                            if (equivalent.ContainsGenericParameters) {
                                                var gmdefTypeArgs = expr.Method.GetGenericArguments();
                                                equivalent = equivalent.MakeGenericMethod(gmdefTypeArgs);
                                            }
                                            return (QueryBuilder)equivalent.Invoke(null, expr.Arguments.Skip(1).Select(a => GetValue(a)).ToArray());
                                        }
                                    }

                                    bool methodHandled = false;
                                    bool methodIsString = expr.Method.DeclaringType == typeof(String);

                                    if (expr.Method.Name == "Any" && expr.Method.DeclaringType == typeof(System.Linq.Enumerable)) {
                                        methodHandled = true;
                                        if (TryDecomposeAnyInvocation(expr, out Expression collection, out LambdaExpression predicate)) {
                                            AggregatePath path;
                                            string alias;
                                            if (TryGetLocalAggregatePath(collection, forcedContext, out AggregatePath localPath)) {
                                                path = localPath;
                                                alias = _resolver.Resolve(path);
                                            } else {
                                                path = GetAggregatePath(collection);
                                                alias = _resolver.Resolve(path);
                                            }
                                            string identifier = _resolver.ResolveTable(path).Identifier.ColumnName;
                                            strBuilder.Append($"{alias}.{identifier} IS NOT NULL");

                                            if (predicate != null && predicate.Parameters.Count == 1) {
                                                ForcedCollectionContext collectionContext = GetCollectionContext(collection, predicate, forcedContext);
                                                strBuilder.Append(" AND ");
                                                strBuilder.Append(ParseExpression(predicate.Body, typeOfT, collectionContext.Alias, strBuilder, fullConditions, collectionContext));
                                            }
                                        }
                                    }
                                    if (expr.Method.Name == "Equals" && methodIsString) {
                                        methodHandled = true;
                                        if (expr.Object == null) {
                                            throw new NotSupportedException("Static string.Equals is not supported in condition expressions; use the == operator or instance Equals(string) instead.");
                                        }
                                        var equalsParameters = expr.Method.GetParameters();
                                        if (equalsParameters.Length != 1 || equalsParameters[0].ParameterType != typeof(string)) {
                                            throw new NotSupportedException($"string.Equals overload with signature ({String.Join(", ", equalsParameters.Select(p => p.ParameterType.Name))}) is not supported in condition expressions; only instance Equals(string) is supported.");
                                        }
                                        strBuilder.Append("(");
                                        strBuilder.Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append(" = ");
                                        strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.Contains) && methodIsString) {
                                        methodHandled = true;
                                        var containsParameters = expr.Method.GetParameters();
                                        if (containsParameters.Length != 1 || containsParameters[0].ParameterType != typeof(string)) {
                                            throw new NotSupportedException($"string.Contains overload with signature ({String.Join(", ", containsParameters.Select(p => p.ParameterType.Name))}) is not supported in condition expressions; only Contains(string) is supported.");
                                        }
                                        strBuilder.Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append($" LIKE CONCAT('%', @{GenerateParameterId}, '%') ESCAPE '!'", EscapeLikeLiteral((string)GetValue(expr.Arguments[0])));
                                    }
                                    if (expr.Method.Name == nameof(String.StartsWith) && methodIsString) {
                                        methodHandled = true;
                                        var startsWithParameters = expr.Method.GetParameters();
                                        if (startsWithParameters.Length != 1 || startsWithParameters[0].ParameterType != typeof(string)) {
                                            throw new NotSupportedException($"string.StartsWith overload with signature ({String.Join(", ", startsWithParameters.Select(p => p.ParameterType.Name))}) is not supported in condition expressions; only StartsWith(string) is supported.");
                                        }
                                        strBuilder.Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append($" LIKE CONCAT(@{GenerateParameterId}, '%') ESCAPE '!'", EscapeLikeLiteral((string)GetValue(expr.Arguments[0])));
                                    }
                                    if (expr.Method.Name == nameof(String.EndsWith) && methodIsString) {
                                        methodHandled = true;
                                        var endsWithParameters = expr.Method.GetParameters();
                                        if (endsWithParameters.Length != 1 || endsWithParameters[0].ParameterType != typeof(string)) {
                                            throw new NotSupportedException($"string.EndsWith overload with signature ({String.Join(", ", endsWithParameters.Select(p => p.ParameterType.Name))}) is not supported in condition expressions; only EndsWith(string) is supported.");
                                        }
                                        strBuilder.Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append($" LIKE CONCAT('%', @{GenerateParameterId}) ESCAPE '!'", EscapeLikeLiteral((string)GetValue(expr.Arguments[0])));
                                    }
                                    if (expr.Method.Name == nameof(String.ToUpper) && methodIsString) {
                                        methodHandled = true;
                                        if (expr.Arguments.Count != 0) {
                                            throw new NotSupportedException($"string.ToUpper overload with {expr.Arguments.Count} arguments is not supported in condition expressions.");
                                        }
                                        strBuilder.Append($"UPPER(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)).Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.ToLower) && methodIsString) {
                                        methodHandled = true;
                                        if (expr.Arguments.Count != 0) {
                                            throw new NotSupportedException($"string.ToLower overload with {expr.Arguments.Count} arguments is not supported in condition expressions.");
                                        }
                                        strBuilder.Append($"LOWER(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)).Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.Trim) && methodIsString) {
                                        methodHandled = true;
                                        if (expr.Arguments.Count != 0) {
                                            throw new NotSupportedException($"string.Trim overload with {expr.Arguments.Count} arguments is not supported in condition expressions.");
                                        }
                                        strBuilder.Append($"TRIM(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)).Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.Replace) && methodIsString) {
                                        methodHandled = true;
                                        var replaceParameters = expr.Method.GetParameters();
                                        if (replaceParameters.Length != 2 || replaceParameters[0].ParameterType != typeof(string) || replaceParameters[1].ParameterType != typeof(string)) {
                                            throw new NotSupportedException($"string.Replace overload with signature ({String.Join(", ", replaceParameters.Select(p => p.ParameterType.Name))}) is not supported in condition expressions; only Replace(string, string) is supported.");
                                        }
                                        strBuilder
                                            .Append($"REPLACE(")
                                            .Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext))
                                            .Append(",")
                                            .Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext))
                                            .Append(",")
                                            .Append(ParseExpression(expr.Arguments[1], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext))
                                            .Append(")");
                                    }
                                    if (expr.Method.Name == "Where" && expr.Method.DeclaringType == typeof(System.Linq.Enumerable)) {
                                        methodHandled = true;
                                        if (expr.Arguments.Count > 1) {
                                            var predicate = expr.Arguments[1] as LambdaExpression;
                                            var collectionContext = GetCollectionContext(expr.Arguments[0], predicate, forcedContext);
                                            strBuilder.Append(ParseExpression(predicate.Body, typeOfT, collectionContext.Alias, strBuilder, fullConditions, collectionContext));
                                        }
                                    }
                                    if (expr.Method.Name == "First" && expr.Method.DeclaringType == typeof(System.Linq.Enumerable)) {
                                        methodHandled = true;
                                        if (expr.Arguments.Count == 1 && expr.Object == null) {
                                            // Enumerable.First(collection)
                                            if (TryGetLocalAggregatePath(expr.Arguments[0], forcedContext, out AggregatePath localPath)) {
                                                strBuilder.Append(_resolver.Resolve(localPath));
                                            } else {
                                                strBuilder.Append(GetPrefixOfExpression(expr.Arguments[0]));
                                            }
                                        } else if (expr.Arguments.Count == 0 && expr.Object != null) {
                                            // collection.First()
                                            if (TryGetLocalAggregatePath(expr.Object, forcedContext, out AggregatePath localPath)) {
                                                strBuilder.Append(_resolver.Resolve(localPath));
                                            } else {
                                                strBuilder.Append(GetPrefixOfExpression(expr.Object));
                                            }
                                        } else {
                                            throw new NotSupportedException("First(predicate) is not supported in condition expressions; use Any(predicate) or Where(predicate).Any() instead.");
                                        }
                                    }
                                    if (!methodHandled && TryBuildMembershipCondition(expr, typeOfT, ForceAlias, fullConditions, forcedContext, false, out QueryBuilder membershipBuilder)) {
                                        methodHandled = true;
                                        strBuilder.Append(membershipBuilder);
                                    }
                                    if (!methodHandled) {
                                        throw new NotSupportedException($"Method call '{expr.Method.Name}' on type '{expr.Method.DeclaringType?.Name}' is not supported in condition expressions.");
                                    }
                                }
            if (foofun is NewExpression newex) {
                var marshalledValue = newex.Constructor.Invoke(newex.Arguments.Select(arg => GetValue(arg)).ToArray());
                return Qb.Fmt($"@{GenerateParameterId}", marshalledValue);
            }
            if (fullConditions) {
                return strBuilder;
            } else {
                string rootPrefix = GetRootAlias() + ".";
                return (QueryBuilder)new QueryBuilder().Append(RemoveExactQualifiedPrefix(strBuilder.GetCommandText(), rootPrefix), strBuilder.GetParameters().Select((pm) => pm.Value).ToArray());
            }
        }
    }
}
