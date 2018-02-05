using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Helpers;
using Figlotech.BDados;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Figlotech.Core;
using Figlotech.Core.Helpers;

namespace Figlotech.BDados.Helpers {
    public class ConditionParser {
        private Type rootType;

        private PrefixMaker prefixer = new PrefixMaker();
        public ConditionParser() {

        }
        public ConditionParser(PrefixMaker prefixMaker) {
            prefixer = prefixMaker;
        }

        private String GetPrefixOfFarField(Expression expression, AggregateFarFieldAttribute info) {
            if (expression == null)
                return "";
            var s = expression.ToString().Split('.');
            String rootType = "";
            Expression subexp = expression;
            while (subexp.NodeType == ExpressionType.MemberAccess) {
                if (subexp is MemberExpression)
                    subexp = (subexp as MemberExpression).Expression;
            }
            rootType = subexp.Type.Name;
            int i = -1;
            var thisAlias = "root";
            s[0] = rootType;
            while (++i < s.Length - 1) {
                thisAlias = prefixer.GetAliasFor(thisAlias, s[i]);
            }
            thisAlias = prefixer.GetAliasFor(thisAlias, info.ImediateKey);
            thisAlias = prefixer.GetAliasFor(thisAlias, info.FarKey);

            return thisAlias;
        }

        private String GetPrefixOfAgField(Expression expression, AggregateFieldAttribute info) {
            if (expression == null)
                return "";
            var s = expression.ToString().Split('.');
            String rootType = "";
            Expression subexp = expression;
            while (subexp.NodeType == ExpressionType.MemberAccess) {
                if (subexp is MemberExpression)
                    subexp = (subexp as MemberExpression).Expression;
            }
            rootType = subexp.Type.Name;
            int i = -1;
            var thisAlias = "root";
            s[0] = rootType;
            while (++i < s.Length - 1) {
                thisAlias = prefixer.GetAliasFor(thisAlias, s[i]);
            }
            thisAlias = prefixer.GetAliasFor(thisAlias, info.ObjectKey);

            return thisAlias;
        }

        private String GetPrefixOfAgObj(Expression expression, AggregateObjectAttribute info) {
            if (expression == null)
                return "";
            var s = expression.ToString().Split('.');
            String rootType = "";
            Expression subexp = expression;
            while (subexp.NodeType == ExpressionType.MemberAccess) {
                if (subexp is MemberExpression)
                    subexp = (subexp as MemberExpression).Expression;
            }
            rootType = subexp.Type.Name;
            int i = -1;
            var thisAlias = "root";
            s[0] = rootType;
            while (++i < s.Length - 1) {
                thisAlias = prefixer.GetAliasFor(thisAlias, s[i]);
            }
            thisAlias = prefixer.GetAliasFor(thisAlias, s[s.Length - 1]);

            return thisAlias;
        }

        private String GetPrefixOf(Expression expression) {
            if (expression == null)
                return "";
            var s = expression.ToString().Split('.');
            String rootType = "";
            Expression subexp = expression;
            while (subexp.NodeType == ExpressionType.MemberAccess) {
                if (subexp is MemberExpression)
                    subexp = (subexp as MemberExpression).Expression;
            }
            rootType = subexp.Type.Name;
            int i = -1;
            var thisAlias = "root";
            s[0] = rootType;
            while (++i < s.Length) {
                thisAlias = prefixer.GetAliasFor(thisAlias, s[i]);
            }

            return thisAlias;
        }


        public QueryBuilder ParseExpression<T>(Conditions<T> c) {
            return ParseExpression(c.expression, typeof(T));
        }

        public QueryBuilder ParseExpression<T>(Expression<Func<T, bool>> foofun, bool fullConditions = true, QueryBuilder strBuilder = null) {
            if (foofun == null) {
                return new QbFmt("TRUE");
            }
            rootType = typeof(T);
            return ParseExpression(foofun.Body, typeof(T), null, strBuilder, fullConditions);
        }

        private bool CanGetValue(Expression member) {
            try {
                GetValue(member);
                return true;
            } catch (Exception) {
                return false;
            }
        }

        private object GetValue(Expression member) {
            var objectMember = Expression.Convert(member, typeof(object));
            
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);

            var getter = getterLambda.Compile();

            return getter();
        }

        private QueryBuilder ParseExpression(Expression foofun, Type typeOfT, String ForceAlias = null, QueryBuilder strBuilder = null, bool fullConditions = true) {
            //if(strBuilder == null)
            strBuilder = new QueryBuilder();

            if (foofun is BinaryExpression) {
                var expr = foofun as BinaryExpression;
                if (!fullConditions) {
                    var mexp = expr.Left as MemberExpression;
                    Expression subexp = mexp;
                    while (subexp is MemberExpression && (subexp as MemberExpression).Expression != null)
                        subexp = (subexp as MemberExpression).Expression;
                    if (subexp is ParameterExpression) {
                        // If this member belongs to an AggregateField, then problems problems...
                        var aList = new List<MemberInfo>();
                        aList.AddRange(ReflectionTool.FieldsWithAttribute<FieldAttribute>(subexp.Type).Where((t) => t.Name == mexp.Member.Name));
                        if (!fullConditions && ((subexp as ParameterExpression).Type == typeOfT || !aList.Any())) {
                            return new QbFmt("TRUE");
                        }
                    }
                }
                if (expr.NodeType == ExpressionType.Equal &&
                    expr.Right is ConstantExpression && (expr.Right as ConstantExpression).Value == null) {
                    strBuilder.Append("(");
                    strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append("IS NULL");
                    strBuilder.Append(")");
                } else
                if (expr.NodeType == ExpressionType.NotEqual &&
                    expr.Right is ConstantExpression && (expr.Right as ConstantExpression).Value == null) {
                    strBuilder.Append("(");
                    strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append("IS NOT NULL");
                    strBuilder.Append(")");
                } else
                if (expr.NodeType == ExpressionType.Equal &&
                    CanGetValue(expr.Right) &&
                    GetValue(expr.Right).GetType() == typeof(string) &&
                    (expr.Left is MemberExpression)
                ) {
                    var comparisonType = (expr.Left as MemberExpression).Member.GetCustomAttribute<QueryComparisonAttribute>()?.Type;
                    if (GetValue(expr.Right).GetType() == typeof(string)) {
                        strBuilder.Append("(");
                        strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions));
                        strBuilder.Append("LIKE");
                        var appendFragment = String.Empty;
                        switch (comparisonType) {
                            case DataStringComparisonType.Containing:
                                appendFragment = $"CONCAT('%', @{IntEx.GenerateShortRid()}, '%')";
                                break;
                            case DataStringComparisonType.EndingWith:
                                appendFragment = $"CONCAT('%', @{IntEx.GenerateShortRid()})";
                                break;
                            case DataStringComparisonType.StartingWith:
                                appendFragment = $"CONCAT(@{IntEx.GenerateShortRid()}, '%')";
                                break;
                            case DataStringComparisonType.ExactValue:
                                appendFragment = $"@{IntEx.GenerateShortRid()}";
                                break;
                            default:
                                appendFragment = $"@{IntEx.GenerateShortRid()}";
                                break;
                        }
                        strBuilder.Append(appendFragment, GetValue(expr.Right));
                        strBuilder.Append(")");
                    }
                } else {
                    strBuilder.Append("(");
                    strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions));
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
                    strBuilder.Append(ParseExpression(expr.Right, typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append(")");
                }
            } else
            if (foofun is MemberExpression) {
                var expr = foofun as MemberExpression;
                Expression subexp = expr;
                while (subexp is MemberExpression && (subexp as MemberExpression).Expression != null)
                    subexp = (subexp as MemberExpression).Expression;
                if(subexp is UnaryExpression) {
                    subexp = (subexp as UnaryExpression).Operand;
                }
                if (subexp is ParameterExpression) {
                    // If this member belongs to an AggregateField, then problems problems...
                    var aList = new List<MemberInfo>();
                    aList.AddRange(ReflectionTool.FieldsWithAttribute<FieldAttribute>(subexp.Type).Where((t) => t.Name == expr.Member.Name.Replace("_","")));
                    if (!fullConditions && ((subexp as ParameterExpression).Type == typeOfT || !aList.Any())) {
                        return new QbFmt("1");
                    }
                    if (aList.Any()) {
                        strBuilder.Append($"{ForceAlias ?? GetPrefixOf(expr.Expression)}.{expr.Member.Name.Replace("_", "")}");
                    } else {
                        // oh hell.
                        MemberInfo member;
                        if(expr.Expression is ParameterExpression) {
                            member = (expr.Expression as ParameterExpression).Type.GetMembers().FirstOrDefault(m => m.Name == expr.Member.Name);
                        } else {
                            member = (expr.Expression as MemberExpression).Type.GetMembers().FirstOrDefault(m => m.Name == expr.Member.Name);
                        }
                        var info = ReflectionTool.GetAttributeFrom<AggregateFieldAttribute>(member);
                        if (info != null) {
                            var prefix = ForceAlias ?? GetPrefixOfAgField(expr, info); // prefixer.GetAliasFor("root", subexp.Type.Name);
                            //var alias = prefixer.GetAliasFor(prefix, expr.Member.Name);
                            strBuilder.Append($"{prefix}.{info.RemoteField}");
                            Fi.Tech.WriteLine(info.ToString());
                        } else {
                            var info2 = ReflectionTool.GetAttributeFrom<AggregateFarFieldAttribute>(expr.Member);
                            if (info2 != null) {
                                var prefix = ForceAlias ?? GetPrefixOfFarField(expr, info2); // prefixer.GetAliasFor("root", subexp.Type.Name);
                                                                                             //var alias = prefixer.GetAliasFor(prefix, expr.Member.Name);
                                strBuilder.Append($"{prefix}.{info2.FarField}");
                            } else {
                                var mem = (expr.Expression).Type.GetMembers().FirstOrDefault(m=>m.Name == expr.Member.Name);
                                var info3 = ReflectionTool.GetAttributeFrom<AggregateObjectAttribute>(mem);
                                if (info3 != null) {
                                    var prefix = ForceAlias ?? GetPrefixOfAgObj(expr.Expression, info3); // prefixer.GetAliasFor("root", subexp.Type.Name);
                                                                                                                                                 //var alias = prefixer.GetAliasFor(prefix, expr.Member.Name);
                                    strBuilder.Append($"{prefix}.{expr.Member.Name}");
                                    Fi.Tech.WriteLine(info3.ToString());
                                } else {
                                    var prefix = GetPrefixOf(expr.Expression);
                                    strBuilder.Append($"{prefix}.{expr.Member.Name}");
                                }

                            }
                        }
                    }
                } else if (subexp is MethodCallExpression) {
                    strBuilder.Append($"{ParseExpression(subexp, typeOfT, ForceAlias, strBuilder, fullConditions).GetCommandText()}.{expr.Member.Name}");
                } else {
                    strBuilder.Append($"@{IntEx.GenerateShortRid()}", GetValue(expr));
                }
            } else
            if (foofun is ConstantExpression) {
                var expr = foofun as ConstantExpression;
                strBuilder.Append($"@{IntEx.GenerateShortRid()}", expr.Value);
            } else
            if (foofun is UnaryExpression) {
                var expr = foofun as UnaryExpression;
                if (expr.NodeType == ExpressionType.Not) {
                    strBuilder.Append("!(");
                    strBuilder.Append(ParseExpression(expr.Operand, typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append(")");
                }
                if (expr.NodeType == ExpressionType.Convert) {
                    //strBuilder.Append($"@{IntEx.GerarShortRID()}", GetValue(expr));
                    strBuilder.Append(ParseExpression(expr.Operand, typeOfT, ForceAlias, strBuilder, fullConditions));
                }
            } else
            if (!fullConditions) {
                return new QbFmt("");
            } else
            if (foofun is MethodCallExpression) {
                var expr = foofun as MethodCallExpression;
                if (expr.Method.Name == "Any") {
                    if (expr.Arguments.Count > 0) {
                        if (expr.Arguments[0] is MemberExpression) {
                            strBuilder.Append($"{GetPrefixOf(expr.Arguments[0])}.RID IS NOT NULL");
                        } else {
                            strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions));
                        }
                    }
                }
                if (expr.Method.Name == "ToLower") {
                    strBuilder.Append($"LOWER(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions)).Append(")");
                }
                if (expr.Method.Name == "Where") {
                    if (expr.Arguments.Count > 1) {
                        var prevV = ForceAlias;
                        ForceAlias = GetPrefixOf(expr.Arguments[0] as MemberExpression);
                        strBuilder.Append(ParseExpression((expr.Arguments[1] as LambdaExpression).Body, typeOfT, ForceAlias, strBuilder, fullConditions));
                        ForceAlias = prevV;
                    }
                }
                if (expr.Method.Name == "First") {
                    if (expr.Arguments.Count > 0) {
                        strBuilder.Append(GetPrefixOf(expr.Arguments[0]));
                    }
                }
            } else {

            }
            if (fullConditions) {
                return strBuilder;
            } else {
                return (QueryBuilder) new QueryBuilder().Append(strBuilder.GetCommandText().Replace("a.", ""), strBuilder._objParams.Select((pm) => pm.Value).ToArray());
            }
        }
    }
}
