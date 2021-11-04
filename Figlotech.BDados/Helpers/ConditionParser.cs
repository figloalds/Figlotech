﻿using Figlotech.BDados.DataAccessAbstractions.Attributes;
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
using System.Diagnostics;
using Figlotech.Data;

namespace Figlotech.BDados.Helpers {
    public class ConditionParser {
        private Type rootType;

        private PrefixMaker prefixer = new PrefixMaker();
        public ConditionParser() {

        }
        public ConditionParser(PrefixMaker prefixMaker) {
            prefixer = prefixMaker;
        }

        private String GetPrefixOfExpression(Expression expression) {
            if (expression == null)
                return "";
            if (expression is UnaryExpression u) {
                return GetPrefixOfExpression(u.Operand);
            }
            if (expression is ParameterExpression p) {
                return prefixer.GetAliasFor("root", p.Type.Name, String.Empty);
            }
            var exp = (expression as MemberExpression).Expression;
            var agT = exp.Type;
            Expression subexp = expression;
            var thisAlias = "tba"; // prefixer.GetAliasFor("tba", agT.Name, String.Empty);
            while (subexp.NodeType == ExpressionType.MemberAccess) {
                if (subexp is MemberExpression smex) {
                    var mt = ReflectionTool.GetTypeOf(smex.Member);
                    var smexMember = (smex.Expression as MemberExpression)?.Member;
                    var f1 = smex.Member.GetCustomAttribute<AggregateFieldAttribute>();
                    var f2 = smexMember?.GetCustomAttribute<AggregateObjectAttribute>();
                    var f3 = smex.Member.GetCustomAttribute<AggregateFarFieldAttribute>();
                    var f4 = smexMember?.GetCustomAttribute<AggregateListAttribute>();
                    if(f1 != null) {
                        thisAlias = prefixer.GetAliasFor(thisAlias, f1.RemoteObjectType.Name, f1.ObjectKey);
                    } else if (f2 != null) {
                        thisAlias = prefixer.GetAliasFor(thisAlias, ReflectionTool.GetTypeOf(smexMember)?.Name, f2.ObjectKey);
                    } else if (f3 != null) {
                        thisAlias = prefixer.GetAliasFor(thisAlias, f3.ImediateType.Name, f3.ImediateKey);
                        thisAlias = prefixer.GetAliasFor(thisAlias, f3.FarType.Name, f3.FarKey);
                    } else if (f4 != null) {
                        thisAlias = prefixer.GetAliasFor(thisAlias, f4.RemoteObjectType.Name, f4.RemoteField);
                    }
                    subexp = (subexp as MemberExpression).Expression;
                }
            }

            return thisAlias;
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
                var retv = ParseExpression(c.expression, typeof(T));
                return retv;
            } catch(Exception x) {
                throw new BDadosException($"Expression parsing failed for Conditions<T> {c?.expression?.ToString()}", x);
            }
        }

        public QueryBuilder ParseExpression<T>(Expression<Func<T, bool>> foofun, bool fullConditions = true, QueryBuilder strBuilder = null) {
            try {
                if (foofun == null) {
                    return new QbFmt("TRUE");
                }
                rootType = typeof(T);
                var retv = ParseExpression(foofun.Body, typeof(T), null, strBuilder, fullConditions);
                return retv;
            } catch(Exception x) {
                throw new BDadosException($"Expression parsing failed for {foofun?.ToString()}", x);
            }
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
            //if (member is MemberExpression memex) {
            //    return GetValue(memex.Expression);
            //}
            try {
                var objectMember = Expression.Convert(member, typeof(object));

                var getterLambda = Expression.Lambda<Func<object>>(objectMember);

                var getter = getterLambda.Compile();

                return getter();
            } catch(NullReferenceException nref) {
                Fi.Tech.WriteLine("ConditionParser", $"NullReferenceException at Parser for member {member?.ToString()}");
                return null;
            }
        }

        int idGeneration = -1;
        string GenerateParameterId => $"_p{++idGeneration}";

        private QueryBuilder ParseExpression(Expression foofun, Type typeOfT, String ForceAlias = null, QueryBuilder strBuilder = null, bool fullConditions = true) {
            //if(strBuilder == null)
            strBuilder = new QueryBuilder();

            if (foofun is BinaryExpression) {
                var expr = foofun as BinaryExpression;
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
                    GetValue(expr.Right)?.GetType() == typeof(string) &&
                    (expr.Left is MemberExpression)
                ) {
                    var member = (expr.Left as MemberExpression).Member;
                    var comparisonType = member.GetCustomAttribute<QueryComparisonAttribute>()?.Type;
                    //if (Debugger.IsAttached) {
                    //    Debugger.Break();
                    //}
                    if (GetValue(expr.Right)?.GetType() == typeof(string)) {
                        strBuilder.Append("(");
                        strBuilder.Append(ParseExpression(expr.Left, typeOfT, ForceAlias, strBuilder, fullConditions));
                        var appendFragment = String.Empty;
                        switch (comparisonType) {
                            case DataStringComparisonType.Containing:
                                appendFragment = $"LIKE CONCAT('%', @{GenerateParameterId}, '%')";
                                break;
                            case DataStringComparisonType.EndingWith:
                                appendFragment = $"LIKE CONCAT('%', @{GenerateParameterId})";
                                break;
                            case DataStringComparisonType.StartingWith:
                                appendFragment = $"LIKE CONCAT(@{GenerateParameterId}, '%')";
                                break;
                            case DataStringComparisonType.ExactValue:
                                appendFragment = $"=@{GenerateParameterId}";
                                break;
                            default:
                                appendFragment = $"=@{GenerateParameterId}";
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
                    if (!fullConditions && ((subexp as ParameterExpression).Type != typeOfT || !aList.Any())) {
                        return new QbFmt("1");
                    }
                    if (aList.Any()) {
                        strBuilder.Append($"{ForceAlias ?? GetPrefixOfExpression(expr.Expression)}.{expr.Member.Name.Replace("_", "")}");
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
                            var prefix = ForceAlias ?? GetPrefixOfExpression(expr); // prefixer.GetAliasFor("root", subexp.Type.Name);
                            //var alias = prefixer.GetAliasFor(prefix, expr.Member.Name);
                            strBuilder.Append($"{prefix}.{info.RemoteField}");
                        } else {
                            var info2 = ReflectionTool.GetAttributeFrom<AggregateFarFieldAttribute>(expr.Member);
                            if (info2 != null) {
                                var prefix = ForceAlias ?? GetPrefixOfExpression(expr); // prefixer.GetAliasFor("root", subexp.Type.Name);
                                                                                             //var alias = prefixer.GetAliasFor(prefix, expr.Member.Name);
                                strBuilder.Append($"{prefix}.{info2.FarField}");
                            } else {
                                var mem = (expr.Expression).Type.GetMembers().FirstOrDefault(m=>m.Name == expr.Member.Name);
                                if(mem == null) {
                                    throw new BDadosException($"Fatal runtime inconsistency error: Cannot find member {expr.Member.Name} in type {(expr.Expression).Type}!");
                                }
                                var info3 = ReflectionTool.GetAttributeFrom<AggregateObjectAttribute>(mem);
                                var altName = ReflectionTool.GetAttributeFrom<OverrideColumnNameOnWhere>(mem);
                                var memberName = altName?.Name ?? member.Name;
                                if (info3 != null) {
                                    var prefix = ForceAlias ?? GetPrefixOfExpression(expr.Expression); // prefixer.GetAliasFor("root", subexp.Type.Name);
                                    //var alias = prefixer.GetAliasFor(prefix, expr.Member.Name);
                                    strBuilder.Append($"{prefix}.{memberName}");
                                } else {
                                    var prefix = GetPrefixOfExpression(expr);
                                    strBuilder.Append($"{prefix}.{memberName}");
                                }

                            }
                        }
                    }
                } else if (subexp is MethodCallExpression) {
                    strBuilder.Append($"{ParseExpression(subexp, typeOfT, ForceAlias, strBuilder, fullConditions).GetCommandText()}.{expr.Member.Name}");
                } else {
                    strBuilder.Append($"@{GenerateParameterId}", GetValue(expr));
                }
            } else
            if (foofun is ConstantExpression) {
                var expr = foofun as ConstantExpression;
                strBuilder.Append($"@{GenerateParameterId}", expr.Value);
            } else
            if (foofun is UnaryExpression) {
                var expr = foofun as UnaryExpression;
                if (expr.NodeType == ExpressionType.Not) {
                    strBuilder.Append("!(");
                    strBuilder.Append(ParseExpression(expr.Operand, typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append(")");
                }
                if (expr.NodeType == ExpressionType.Convert) {
                    //strBuilder.Append($"@{GenerateParameterId}", GetValue(expr));
                    strBuilder.Append(ParseExpression(expr.Operand, typeOfT, ForceAlias, strBuilder, fullConditions));
                }
            } else
            if (!fullConditions) {
                return new QbFmt("");
            } else
            if (foofun is MethodCallExpression) {
                var expr = foofun as MethodCallExpression;

                if(expr.Method.DeclaringType == typeof(Qh)) {
                    var tq = typeof(Qb);
                    var equivalent = tq.GetMethods().FirstOrDefault(m => m.Name == expr.Method.Name && m.GetParameters().Length == expr.Method.GetParameters().Length - 1);
                    if(equivalent != null) {
                        if(equivalent.ContainsGenericParameters) {
                            var gmdefTypeArgs = expr.Method.GetGenericArguments();
                            //var gmdefTypeArgs = gmdef;
                            equivalent = equivalent.MakeGenericMethod(gmdefTypeArgs);
                        }
                        return (QueryBuilder) equivalent.Invoke(null, expr.Arguments.Skip(1).Select(a=> GetValue(a)).ToArray());
                    }
                }

                if (expr.Method.Name == "Any") {
                    if (expr.Arguments.Count > 0) {
                        if (expr.Arguments[0] is MemberExpression) {
                            strBuilder.Append($"{GetPrefixOfExpression(expr.Arguments[0])}.RID IS NOT NULL");
                        } else {
                            strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions));
                        }
                    }
                }
                if (expr.Method.Name == "ToLower") {
                    strBuilder.Append($"LOWER(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions)).Append(")");
                }
                if (expr.Method.Name == "Equals") {
                    var memberEx = expr.Object as MemberExpression;
                    var pre = GetPrefixOfExpression(memberEx);
                    var column = memberEx.Member.Name;
                    strBuilder.Append($"{pre}.{column}=(");
                    strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append(")");
                }
                if (expr.Method.Name == "Contains") {
                    var memberEx = expr.Object as MemberExpression;
                    var pre = GetPrefixOfExpression(memberEx);
                    var column = memberEx.Member.Name;
                    strBuilder.Append($"{pre}.{column} LIKE CONCAT('%', ");
                    strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append(", '%')");
                }
                if (expr.Method.Name == "StartsWith") {
                    var memberEx = expr.Object as MemberExpression;
                    var pre = GetPrefixOfExpression(memberEx);
                    var column = memberEx.Member.Name;
                    strBuilder.Append($"{pre}.{column} LIKE CONCAT('%', ");
                    strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append(")");
                }
                if (expr.Method.Name == "EndsWith") {
                    var memberEx = expr.Object as MemberExpression;
                    var pre = GetPrefixOfExpression(memberEx);
                    var column = memberEx.Member.Name;
                    strBuilder.Append($"{pre}.{column} LIKE CONCAT(");
                    strBuilder.Append(ParseExpression(expr.Arguments[0], typeOfT, ForceAlias, strBuilder, fullConditions));
                    strBuilder.Append(", '%')");
                }
                if (expr.Method.Name == "ToUpper") {
                    strBuilder.Append($"UPPER(").Append(ParseExpression(expr.Object, typeOfT, ForceAlias, strBuilder, fullConditions)).Append(")");
                }
                if (expr.Method.Name == "Where") {
                    if (expr.Arguments.Count > 1) {
                        var prevV = ForceAlias;
                        ForceAlias = GetPrefixOfExpression(expr.Arguments[0] as MemberExpression);
                        strBuilder.Append(ParseExpression((expr.Arguments[1] as LambdaExpression).Body, typeOfT, ForceAlias, strBuilder, fullConditions));
                        ForceAlias = prevV;
                    }
                }
                if (expr.Method.Name == "First") {
                    if (expr.Arguments.Count > 0) {
                        strBuilder.Append(GetPrefixOfExpression(expr.Arguments[0]));
                    }
                }
            }
            if(foofun is NewExpression newex) {
                var marshalledValue = newex.Constructor.Invoke(newex.Arguments.Select(arg => GetValue(arg)).ToArray());
                return Qb.Fmt($"@{GenerateParameterId}", marshalledValue);
            }
            if (fullConditions) {
                return strBuilder;
            } else {
                return (QueryBuilder) new QueryBuilder().Append(strBuilder.GetCommandText().Replace("tba.", ""), strBuilder.GetParameters().Select((pm) => pm.Value).ToArray());
            }
        }
    }
}
