using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Exceptions;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Data;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Figlotech.BDados.Helpers {
    public sealed class ConditionParser {
        private Type rootType;
        private readonly AggregateJoinShape _shape;
        private readonly DefinitiveJoinPlan _configuredPlan;
        private readonly DefinitiveAliasResolver _configuredResolver;
        private readonly PrefixMaker _prefixMaker;
        private DefinitiveAliasResolver _resolver;

        // Static caches for performance optimization
        private static readonly ConcurrentDictionary<Expression, Func<object>> _compiledExpressionCache = new ConcurrentDictionary<Expression, Func<object>>();
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

        //private String GetPrefixOfAgField(Expression expression, AggregateFieldAttribute info) {
        //    if (expression == null)
        //        return "";
        //    var s = expression.ToString().Split('.');
        //    String rootType = "";
        //    Expression subexp = expression;
        //    while (subexp.NodeType == ExpressionType.MemberAccess) {
        //        if (subexp is MemberExpression)
        //            subexp = (subexp as MemberExpression).Expression;
        //    }
        //    rootType = subexp.Type.Name;
        //    int i = -1;
        //    var thisAlias = "root";
        //    s[0] = rootType;
        //    while (++i < s.Length - 1) {
        //        thisAlias = prefixer.GetAliasFor(thisAlias, s[i]);
        //    }
        //    thisAlias = prefixer.GetAliasFor(thisAlias, info.ObjectKey);

        //    return thisAlias;
        //}

        //private String GetPrefixOfAgObj(Expression expression, AggregateObjectAttribute info) {
        //    if (expression == null)
        //        return "";
        //    var s = expression.ToString().Split('.');
        //    String rootType = "";
        //    Expression subexp = expression;
        //    while (subexp.NodeType == ExpressionType.MemberAccess) {
        //        if (subexp is MemberExpression)
        //            subexp = (subexp as MemberExpression).Expression;
        //    }
        //    rootType = subexp.Type.Name;
        //    int i = -1;
        //    var thisAlias = "root";
        //    s[0] = rootType;
        //    while (++i < s.Length - 1) {
        //        thisAlias = prefixer.GetAliasFor(thisAlias, s[i]);
        //    }
        //    thisAlias = prefixer.GetAliasFor(thisAlias, s[s.Length - 1]);

        //    return thisAlias;
        //}

        //private String GetPrefixOf(Expression expression) {
        //    if (expression == null)
        //        return "";
        //    var s = expression.ToString().Split('.');
        //    String rootType = "";
        //    Expression subexp = expression;
        //    while (subexp.NodeType == ExpressionType.MemberAccess) {
        //        if (subexp is MemberExpression) {
        //            subexp = (subexp as MemberExpression).Expression;
        //        }
        //    }
        //    if (subexp is UnaryExpression un && subexp.NodeType == ExpressionType.Convert) {
        //        subexp = un.Operand;
        //    }
        //    rootType = subexp.Type.Name;
        //    int i = -1;
        //    var thisAlias = "root";
        //    s[0] = rootType;
        //    while (++i < s.Length) {
        //        thisAlias = prefixer.GetAliasFor(thisAlias, s[i]);
        //    }

        //    return thisAlias;
        //}


        public QueryBuilder ParseExpression<T>(Conditions<T> c) {
            try {
                SelectResolver(typeof(T), c.expression);
                var retv = ParseExpression(c.expression, typeof(T));
                return retv;
            } catch (Exception x) {
                throw new BDadosException($"Expression parsing failed for Conditions<T> {c?.expression?.ToString()}", x);
            }
        }

        public QueryBuilder ParseExpression<T>(Expression<Func<T, bool>> foofun, bool fullConditions = true, QueryBuilder strBuilder = null) {
            try {
                if (foofun == null) {
                    return new QbFmt("TRUE");
                }
                SelectResolver(typeof(T), foofun.Body);
                if (!fullConditions) {
                    RootConditionProjection projection = ProjectRootCondition(foofun.Body);
                    if (IsTrue(projection.Expression)) {
                        return new QbFmt("TRUE");
                    }
                    QueryBuilder projected = ParseExpression(projection.Expression, typeof(T), null, strBuilder, true, forcedContext: null);
                    string rootPrefix = GetRootAlias() + ".";
                    return (QueryBuilder)new QueryBuilder().Append(
                        RemoveExactQualifiedPrefix(projected.GetCommandText(), rootPrefix),
                        projected.GetParameters().Select(parameter => parameter.Value).ToArray());
                }
                var retv = ParseExpression(foofun.Body, typeof(T), null, strBuilder, fullConditions, forcedContext: null);
                return retv;
            } catch (Exception x) {
                throw new BDadosException($"Expression parsing failed for {foofun?.ToString()}", x);
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
                try {
                    return cachedGetter();
                } catch (NullReferenceException) {
                    Fi.Tech.WriteLine("ConditionParser", $"NullReferenceException at Parser for cached member {member?.ToString()}");
                    return null;
                } catch (InvalidOperationException ioex) {
                    if (ioex.Message == "Nullable object must have a value.") {
                        return null;
                    }
                    return null;
                } catch {
                    return null;
                }
            }

            try {
                var objectMember = Expression.Convert(member, typeof(object));
                var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                var getter = getterLambda.Compile();

                // Cache the compiled delegate
                _compiledExpressionCache.TryAdd(member, getter);

                return getter();
            } catch (NullReferenceException) {
                Fi.Tech.WriteLine("ConditionParser", $"NullReferenceException at Parser for member {member?.ToString()}");
                return null;
            } catch {
                return null;
            }
        }

        int idGeneration = -1;
        string GenerateParameterId => $"_p{++idGeneration}";

        private QueryBuilder ParseExpression(Expression foofun, Type typeOfT, String ForceAlias = null, QueryBuilder strBuilder = null, bool fullConditions = true, ForcedCollectionContext forcedContext = null) {
            //if(strBuilder == null)
            strBuilder = new QueryBuilder();

            if (foofun is BinaryExpression bexpr) {
                var expr = bexpr;
                if (expr.NodeType == ExpressionType.Equal &&
                    expr.Right is ConstantExpression rightConst && rightConst.Value == null) {
                    strBuilder.Append("(");
                    strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                    strBuilder.Append("IS NULL");
                    strBuilder.Append(")");
                } else
                    if (expr.NodeType == ExpressionType.NotEqual &&
                        expr.Right is ConstantExpression rightConst2 && rightConst2.Value == null) {
                        strBuilder.Append("(");
                        strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                        strBuilder.Append("IS NOT NULL");
                        strBuilder.Append(")");
                    } else
                        if (expr.NodeType == ExpressionType.Equal &&
                            CanGetValue(expr.Right) &&
                            GetValue(expr.Right) is string rightValue &&
                            (expr.Left is MemberExpression)
                        ) {
                            var member = (expr.Left as MemberExpression).Member;
                            var comparisonType = GetCachedAttribute<QueryComparisonAttribute>(member)?.Type;

                            strBuilder.Append("(");
                            strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                            var appendFragment = comparisonType switch {
                                DataStringComparisonType.Containing => $"LIKE CONCAT('%', @{GenerateParameterId}, '%')",
                                DataStringComparisonType.EndingWith => $"LIKE CONCAT('%', @{GenerateParameterId})",
                                DataStringComparisonType.StartingWith => $"LIKE CONCAT(@{GenerateParameterId}, '%')",
                                DataStringComparisonType.ExactValue => $"=@{GenerateParameterId}",
                                _ => $"=@{GenerateParameterId}"
                            };
                            strBuilder.Append(appendFragment, rightValue);
                            strBuilder.Append(")");
                        } else {
                            strBuilder.Append("(");
                            strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                            strBuilder.Append(
                                expr.NodeType == ExpressionType.AndAlso ? "AND" :
                                expr.NodeType == ExpressionType.OrElse ? "OR" :
                                expr.NodeType == ExpressionType.Equal ? "=" :
                                expr.NodeType == ExpressionType.NotEqual ? "!=" :
                                expr.NodeType == ExpressionType.Not ? "!" :
                                expr.NodeType == ExpressionType.GreaterThan ? ">" :
                                expr.NodeType == ExpressionType.GreaterThanOrEqual ? ">=" :
                                expr.NodeType == ExpressionType.LessThan ? "<" :
                                expr.NodeType == ExpressionType.LessThanOrEqual ? "<=" :
                                "");
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
                                strBuilder.Append("!(");
                                strBuilder.Append(ParseExpression(expr.Operand, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                strBuilder.Append(")");
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
                                    if (expr.Method.DeclaringType == typeof(String)) {
                                        if (expr.Method.Name == nameof(String.Replace)) {
                                            var retv = Qb.Fmt("REPLACE(")
                                                + ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)
                                                + Qb.Fmt(",")
                                                + ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)
                                                + Qb.Fmt(",")
                                                + ParseExpression(expr.Arguments[1], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)
                                                + Qb.Fmt(")");
                                            return retv;
                                        }
                                    }
                                    if (expr.Method.DeclaringType == typeof(Int32) || expr.Method.DeclaringType == typeof(Int64)) {
                                        if (expr.Method.Name == nameof(Int32.Parse)) {
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

                                    if (expr.Method.Name == "Any") {
                                        if (expr.Arguments.Count > 0) {
                                            if (expr.Arguments[0] is MemberExpression) {
                                                var pathExpression = (MemberExpression)expr.Arguments[0];
                                                AggregatePath path;
                                                string alias;
                                                if (TryGetLocalAggregatePath(pathExpression, forcedContext, out AggregatePath localPath)) {
                                                    path = localPath;
                                                    alias = _resolver.Resolve(path);
                                                } else {
                                                    path = GetAggregatePath(pathExpression);
                                                    alias = _resolver.Resolve(path);
                                                }
                                                string identifier = _resolver.ResolveTable(path).Identifier.ColumnName;
                                                strBuilder.Append($"{alias}.{identifier} IS NOT NULL");

                                                LambdaExpression predicate = expr.Arguments.Count > 1
                                                    ? StripExpression(expr.Arguments[1]) as LambdaExpression
                                                    : null;
                                                if (predicate != null && predicate.Parameters.Count == 1) {
                                                    ForcedCollectionContext collectionContext = GetCollectionContext(pathExpression, predicate, forcedContext);
                                                    strBuilder.Append(" AND ");
                                                    strBuilder.Append(ParseExpression(predicate.Body, typeOfT, collectionContext.Alias, strBuilder, fullConditions, collectionContext));
                                                }
                                            } else {
                                                strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                            }
                                        }
                                    }
                                    if (expr.Method.Name == "Equals") {
                                        var memberEx = expr.Object as MemberExpression;
                                        var pre = GetMemberPrefix(memberEx, ForceAlias, forcedContext);
                                        var column = memberEx.Member.Name;
                                        strBuilder.Append($"{pre}.{column}=(");
                                        strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.Contains)) {
                                        strBuilder.Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append($" LIKE CONCAT('%', ");
                                        strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append(", '%')");
                                    }
                                    if (expr.Method.Name == nameof(String.StartsWith)) {
                                        strBuilder.Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append($" LIKE CONCAT('%', ");
                                        strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.EndsWith)) {
                                        strBuilder.Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append($" LIKE CONCAT('%', ");
                                        strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext));
                                        strBuilder.Append(", '%')");
                                    }
                                    if (expr.Method.Name == nameof(String.ToUpper)) {
                                        strBuilder.Append($"UPPER(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)).Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.ToLower)) {
                                        strBuilder.Append($"LOWER(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)).Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.Trim)) {
                                        strBuilder.Append($"TRIM(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext)).Append(")");
                                    }
                                    if (expr.Method.Name == nameof(String.Replace)) {
                                        strBuilder
                                            .Append($"REPLACE(")
                                            .Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext))
                                            .Append(",")
                                            .Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext))
                                            .Append(",")
                                            .Append(ParseExpression(expr.Arguments[1], typeOfT, ForceAlias, strBuilder, fullConditions, forcedContext))
                                            .Append(")");
                                    }
                                    if (expr.Method.Name == "Where") {
                                        if (expr.Arguments.Count > 1) {
                                            var predicate = expr.Arguments[1] as LambdaExpression;
                                            var collectionContext = GetCollectionContext(expr.Arguments[0], predicate, forcedContext);
                                            strBuilder.Append(ParseExpression(predicate.Body, typeOfT, collectionContext.Alias, strBuilder, fullConditions, collectionContext));
                                        }
                                    }
                                    if (expr.Method.Name == "First") {
                                        if (expr.Arguments.Count > 0) {
                                            if (TryGetLocalAggregatePath(expr.Arguments[0], forcedContext, out AggregatePath localPath)) {
                                                strBuilder.Append(_resolver.Resolve(localPath));
                                            } else {
                                                strBuilder.Append(GetPrefixOfExpression(expr.Arguments[0]));
                                            }
                                        }
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
