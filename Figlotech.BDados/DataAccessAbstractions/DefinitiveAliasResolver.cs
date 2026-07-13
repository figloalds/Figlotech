using System;
using System.Collections.Generic;
using System.Linq;

namespace Figlotech.BDados.DataAccessAbstractions {
    /// <summary>
    /// Immutable semantic-path lookup over a definitive join plan.
    /// </summary>
    public sealed class DefinitiveAliasResolver {
        private readonly DefinitiveJoinPlan _plan;

        public DefinitiveAliasResolver(DefinitiveJoinPlan plan) {
            _plan = plan ?? throw new ArgumentNullException(nameof(plan));
        }

        public Type RootType => _plan.RootType;
        public AggregateJoinShape Shape => _plan.Shape;
        public DefinitiveJoinPlan Plan => _plan;

        public string Resolve(AggregatePath path) {
            if (TryResolve(path, out string alias)) {
                return alias;
            }

            string requested = Display(path);
            string known = String.Join(", ", _plan.AliasByPath.OrderBy(entry => entry.Key).Take(16).Select(entry => Display(entry.Key) + "=" + entry.Value));
            throw new ArgumentException($"No frozen alias exists for root type '{RootType}', shape '{Shape}', requested path '{requested}'. Known paths: {known}.", nameof(path));
        }

        public bool TryResolve(AggregatePath path, out string alias) {
            return _plan.AliasByPath.TryGetValue(path, out alias);
        }

        public DefinitiveJoinTable ResolveTable(AggregatePath path) {
            string alias = Resolve(path);
            if (!_plan.TableIndexByAlias.TryGetValue(alias, out int index)) {
                throw new InvalidOperationException($"Frozen plan for root type '{RootType}' has no table index for alias '{alias}'.");
            }
            return _plan.Tables[index];
        }

        private static string Display(AggregatePath path) {
            return path.Segments.Length == 0 ? "<root>" : path.ToString();
        }
    }
}
