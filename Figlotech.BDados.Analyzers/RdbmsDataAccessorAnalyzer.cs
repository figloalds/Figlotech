using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Collections.Generic;
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

            var receiver = GetReceiverSymbol(invocation, context.SemanticModel);
            if (receiver == null) return;

            // Check if this invocation is itself an Access/AccessAsync call (BD001)
            if (AccessMethodNames.Contains(methodSymbol.Name)) {
                var enclosingAccess = FindEnclosingAccessLambda(invocation, context.SemanticModel);
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
                var enclosingAccess = FindEnclosingAccessLambda(invocation, context.SemanticModel);
                if (enclosingAccess.Invocation != null) {
                    var outerReceiver = GetReceiverSymbol(enclosingAccess.Invocation, context.SemanticModel);
                    if (outerReceiver != null && SymbolEqualityComparer.Default.Equals(receiver, outerReceiver)) {
                        // Check if first argument is BDadosTransaction
                        var args = invocation.ArgumentList.Arguments;
                        if (args.Count == 0 || !IsBDadosTransaction(args[0], context.SemanticModel)) {
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

        private ISymbol GetReceiverSymbol(InvocationExpressionSyntax invocation, SemanticModel semanticModel) {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess) {
                return semanticModel.GetSymbolInfo(memberAccess.Expression).Symbol;
            }
            // Implicit this access
            if (invocation.Expression is IdentifierNameSyntax) {
                // Try to get containing type's instance
                var methodDecl = invocation.Ancestors().OfType<MethodDeclarationSyntax>().FirstOrDefault();
                if (methodDecl != null) {
                    var methodSymbol = semanticModel.GetDeclaredSymbol(methodDecl);
                    if (methodSymbol != null) {
                        return methodSymbol.ContainingType;
                    }
                }
            }
            return null;
        }

        private (InvocationExpressionSyntax Invocation, string MethodName) FindEnclosingAccessLambda(
            SyntaxNode node,
            SemanticModel semanticModel) {
            var current = node.Parent;
            while (current != null) {
                if (current is LambdaExpressionSyntax lambda) {
                    // Check if this lambda is an argument to an Access/AccessAsync call
                    var parent = lambda.Parent;
                    if (parent is ArgumentSyntax arg) {
                        var invocation = arg.Ancestors().OfType<InvocationExpressionSyntax>().FirstOrDefault();
                        if (invocation != null) {
                            var symbol = semanticModel.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
                            if (symbol != null && AccessMethodNames.Contains(symbol.Name)) {
                                return (invocation, symbol.Name);
                            }
                        }
                    }
                }
                current = current.Parent;
            }
            return (null, null);
        }

        private bool IsBDadosTransaction(ArgumentSyntax argument, SemanticModel semanticModel) {
            var typeInfo = semanticModel.GetTypeInfo(argument.Expression);
            if (typeInfo.Type == null) return false;
            return typeInfo.Type.ToDisplayString() == "Figlotech.BDados.DataAccessAbstractions.BDadosTransaction";
        }
    }
}
