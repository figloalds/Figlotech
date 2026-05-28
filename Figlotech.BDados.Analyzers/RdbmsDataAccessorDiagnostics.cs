using Microsoft.CodeAnalysis;

namespace Figlotech.BDados.Analyzers {
    public static class RdbmsDataAccessorDiagnostics {
        public const string NestedAccessDiagnosticId = "BD001";
        public const string MissingTransactionDiagnosticId = "BD002";

        public static readonly DiagnosticDescriptor NestedAccess = new DiagnosticDescriptor(
            id: NestedAccessDiagnosticId,
            title: "Nested transaction access",
            messageFormat: "Calling '{0}' on the same accessor instance inside another '{1}' call causes deadlock. Use the BDadosTransaction parameter instead.",
            category: "Reliability",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true,
            description: "Nested Access/AccessAsync calls on the same RdbmsDataAccessor instance can deadlock when the connection pool is exhausted."
        );

        public static readonly DiagnosticDescriptor MissingTransaction = new DiagnosticDescriptor(
            id: MissingTransactionDiagnosticId,
            title: "Missing BDadosTransaction parameter",
            messageFormat: "Method '{0}' should be called with the BDadosTransaction parameter inside an Access/AccessAsync lambda to avoid nested transactions",
            category: "Reliability",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true,
            description: "Calling transaction-spawning convenience methods without passing the transaction parameter causes nested transactions and potential deadlocks."
        );
    }
}
