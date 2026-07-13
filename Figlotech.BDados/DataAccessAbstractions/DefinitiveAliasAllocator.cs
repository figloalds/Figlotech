using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Figlotech.BDados.DataAccessAbstractions {
    /// <summary>
    /// Allocates deterministic SQL aliases and records immutable semantic-path bindings for one compilation.
    /// </summary>
    public sealed class DefinitiveAliasAllocator {
        private readonly Dictionary<RelationshipIdentity, string> _aliases = new Dictionary<RelationshipIdentity, string>();
        private readonly Dictionary<AggregatePath, string> _aliasesByPath = new Dictionary<AggregatePath, string>();
        private int _nextAlias;

        public string GetAlias(string parentAlias, Type childType, string relationshipKey) {
            if (String.IsNullOrWhiteSpace(parentAlias)) {
                throw new ArgumentException("Parent alias must be non-empty.", nameof(parentAlias));
            }
            if (childType == null) {
                throw new ArgumentNullException(nameof(childType));
            }
            if (relationshipKey == null) {
                throw new ArgumentNullException(nameof(relationshipKey));
            }

            var identity = new RelationshipIdentity(parentAlias, childType, relationshipKey);
            if (_aliases.TryGetValue(identity, out string alias)) {
                return alias;
            }

            alias = "tb" + ToBase26(_nextAlias++);
            _aliases.Add(identity, alias);
            return alias;
        }

        public void Bind(AggregatePath path, string alias) {
            if (String.IsNullOrWhiteSpace(alias)) {
                throw new ArgumentException("Alias must be non-empty.", nameof(alias));
            }
            if (_aliasesByPath.TryGetValue(path, out string existing)) {
                if (!String.Equals(existing, alias, StringComparison.OrdinalIgnoreCase)) {
                    throw new ArgumentException($"Aggregate path '{path}' is already bound to alias '{existing}' and cannot be rebound to '{alias}'.", nameof(path));
                }
                return;
            }
            _aliasesByPath.Add(path, alias);
        }

        public ImmutableDictionary<AggregatePath, string> SnapshotAliasesByPath() {
            var builder = ImmutableDictionary.CreateBuilder<AggregatePath, string>();
            foreach (KeyValuePair<AggregatePath, string> binding in _aliasesByPath) {
                builder.Add(binding.Key, binding.Value);
            }
            return builder.ToImmutable();
        }

        private static string ToBase26(int value) {
            if (value < 0) {
                throw new ArgumentOutOfRangeException(nameof(value));
            }

            var characters = new Stack<char>();
            do {
                characters.Push((char)('a' + (value % 26)));
                value /= 26;
            } while (value > 0);
            return new string(characters.ToArray());
        }

        private readonly struct RelationshipIdentity : IEquatable<RelationshipIdentity> {
            private readonly string _parentAlias;
            private readonly Type _childType;
            private readonly string _relationshipKey;

            public RelationshipIdentity(string parentAlias, Type childType, string relationshipKey) {
                _parentAlias = parentAlias;
                _childType = childType;
                _relationshipKey = relationshipKey;
            }

            public bool Equals(RelationshipIdentity other) {
                return _childType == other._childType
                    && String.Equals(_parentAlias, other._parentAlias, StringComparison.OrdinalIgnoreCase)
                    && String.Equals(_relationshipKey, other._relationshipKey, StringComparison.OrdinalIgnoreCase);
            }

            public override bool Equals(object obj) {
                return obj is RelationshipIdentity other && Equals(other);
            }

            public override int GetHashCode() {
                unchecked {
                    int hash = _childType.GetHashCode();
                    hash = (hash * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(_parentAlias);
                    hash = (hash * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(_relationshipKey);
                    return hash;
                }
            }
        }
    }
}
