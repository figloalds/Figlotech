using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Collections.Immutable;
using System.Linq;

namespace Figlotech.BDados.Analyzers {
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public class RdbmsDataAccessorAnalyzer : DiagnosticAnalyzer {
        private static readonly ImmutableArray<string> AccessMethodNames = ImmutableArray.Create(
            "Access",
            "AccessAsync",
            "AccessAsyncCoroutinely"
        );

        private static readonly ImmutableArray<string> ConvenienceMethodNames = ImmutableArray.Create(
            "Query",
            "Execute",
            "LoadById",
            "LoadByRid",
            "LoadAll",
            "SaveItem",
            "Delete",
            "ScalarQuery",
            "ForceExist",
            "Fetch",
            "FetchAsync",
            "LoadFirstOrDefault",
            "LoadFirstOrDefaultAsync",
            "AggregateLoad",
            "AggregateLoadAsync",
            "AggregateLoadAsyncCoroutinely",
            "ExistsByRIDAsync",
            "ExistsByIdAsync",
            "DeleteAsync",
            "DeleteWhereRidNotIn",
            "DeleteWhereRidNotInAsync",
            "SaveList",
            "SaveListAsync",
            "Update",
            "UpdateAndMutate",
            "UpdateAndMutateIfSuccess",
            "UpdateAsync",
            "UpdateAndMutateAsync",
            "UpdateAndMutateIfSuccessAsync"
        );

        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(
            RdbmsDataAccessorDiagnostics.NestedAccess,
            RdbmsDataAccessorDiagnostics.MissingTransaction
        );

        public override void Initialize(AnalysisContext context) {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
        }

        private void AnalyzeInvocation(SyntaxNodeAnalysisContext context) {
            var invocation = (InvocationExpressionSyntax)context.Node;
            var methodSymbol = context.SemanticModel.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
            if (methodSymbol == null) return;

            if (!IsRdbmsDataAccessorMethod(methodSymbol, context.Compilation)) return;

            var receiver = GetReceiverSymbol(invocation, context.SemanticModel);
            if (receiver == null) return;

            var bdadosTransactionType = context.Compilation.GetTypeByMetadataName("Figlotech.BDados.DataAccessAbstractions.BDadosTransaction");

            // Check if this invocation is itself an Access/AccessAsync call (BD001)
            if (AccessMethodNames.Contains(methodSymbol.Name)) {
                var enclosingAccess = FindEnclosingAccessLambda(invocation, invocation, context.SemanticModel);
                if (enclosingAccess.Invocation != null) {
                    var outerReceiver = GetReceiverSymbol(enclosingAccess.Invocation, context.SemanticModel);
                    if (outerReceiver != null && SymbolEqualityComparer.Default.Equals(receiver, outerReceiver)) {
                        var diagnostic = Diagnostic.Create(
                            RdbmsDataAccessorDiagnostics.NestedAccess,
                            invocation.GetLocation(),
                            methodSymbol.Name,
                            enclosingAccess.MethodName
                        );
                        context.ReportDiagnostic(diagnostic);
                    }
                }
            }

            // Check for missing transaction parameter (BD002)
            if (ConvenienceMethodNames.Contains(methodSymbol.Name)) {
                var enclosingAccess = FindEnclosingAccessLambda(invocation, invocation, context.SemanticModel);
                if (enclosingAccess.Invocation != null) {
                    var outerReceiver = GetReceiverSymbol(enclosingAccess.Invocation, context.SemanticModel);
                    if (outerReceiver != null && SymbolEqualityComparer.Default.Equals(receiver, outerReceiver)) {
                        // Check if first argument is BDadosTransaction
                        var args = invocation.ArgumentList.Arguments;
                        if (args.Count == 0 || !IsBDadosTransaction(args[0], context.SemanticModel, bdadosTransactionType)) {
                            var diagnostic = Diagnostic.Create(
                                RdbmsDataAccessorDiagnostics.MissingTransaction,
                                invocation.GetLocation(),
                                methodSymbol.Name
                            );
                            context.ReportDiagnostic(diagnostic);
                        }
                    }
                }
            }
        }

        private bool IsRdbmsDataAccessorMethod(IMethodSymbol methodSymbol, Compilation compilation) {
            var containingType = methodSymbol.ContainingType;
            if (containingType == null) return false;

            var irdbmsInterface = compilation.GetTypeByMetadataName("Figlotech.BDados.DataAccessAbstractions.IRdbmsDataAccessor");
            var rdbmsClass = compilation.GetTypeByMetadataName("Figlotech.BDados.DataAccessAbstractions.RdbmsDataAccessor");

            if (rdbmsClass != null && SymbolEqualityComparer.Default.Equals(containingType, rdbmsClass)) {
                return true;
            }

            if (irdbmsInterface != null) {
                foreach (var iface in containingType.AllInterfaces) {
                    if (SymbolEqualityComparer.Default.Equals(iface, irdbmsInterface)) {
                        return true;
                    }
                }
            }

            var baseType = containingType.BaseType;
            while (baseType != null) {
                if (rdbmsClass != null && SymbolEqualityComparer.Default.Equals(baseType, rdbmsClass)) {
                    return true;
                }
                baseType = baseType.BaseType;
            }

            return false;
        }

        private ISymbol GetReceiverSymbol(InvocationExpressionSyntax invocation, SemanticModel semanticModel) {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess) {
                return semanticModel.GetSymbolInfo(memberAccess.Expression).Symbol;
            }
            // Implicit this access
            if (invocation.Expression is IdentifierNameSyntax) {
                var enclosingSymbol = semanticModel.GetEnclosingSymbol(invocation.SpanStart);
                if (enclosingSymbol != null) {
                    return enclosingSymbol.ContainingType;
                }
            }
            return null;
        }

        private (InvocationExpressionSyntax Invocation, string MethodName) FindEnclosingAccessLambda(
            SyntaxNode node,
            InvocationExpressionSyntax skipInvocation,
            SemanticModel semanticModel) {
            var current = node.Parent;
            while (current != null) {
                if (current is LambdaExpressionSyntax lambda) {
                    // Check if this lambda is an argument to an Access/AccessAsync call
                    var parent = lambda.Parent;
                    if (parent is ArgumentSyntax arg) {
                        var invocation = arg.Ancestors().OfType<InvocationExpressionSyntax>().FirstOrDefault();
                        if (invocation != null && invocation != skipInvocation) {
                            var symbol = semanticModel.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
                            if (symbol != null && AccessMethodNames.Contains(symbol.Name) && IsRdbmsDataAccessorMethod(symbol, semanticModel.Compilation)) {
                                return (invocation, symbol.Name);
                            }
                        }
                    }
                }
                current = current.Parent;
            }
            return (null, null);
        }

        private bool IsBDadosTransaction(ArgumentSyntax argument, SemanticModel semanticModel, INamedTypeSymbol bdadosTransactionType) {
            if (bdadosTransactionType == null) return false;
            var typeInfo = semanticModel.GetTypeInfo(argument.Expression);
            if (typeInfo.Type == null) return false;
            return SymbolEqualityComparer.Default.Equals(typeInfo.Type, bdadosTransactionType);
        }
    }
}
