using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class TypeDepScore {
        public Type Type { get; set; }
        public int DepScore { get; set; } = 0;
        bool IsMeasured = false;

        public TypeDepScore(Type t) {
            Type = t;
        }

        public static IEnumerable<Type> ExplodeDependencyTree(Type t, int level = 0, List<Type> dependedTypes = null) {
            var members = ReflectionTool.FieldsAndPropertiesOf(t);
            dependedTypes = dependedTypes ?? new List<Type>();
            //Console.WriteLine($"EDT | {new String(' ', level)} -> {t.Name}");
            foreach (var m in members) {
                var fk = m.GetCustomAttribute<ForeignKeyAttribute>();
                if (fk != null) {
                    if (!dependedTypes.Contains(fk.RefType)) {
                        dependedTypes.Add(fk.RefType);
                        yield return (fk.RefType);
                        foreach (var innerDep in ExplodeDependencyTree(fk.RefType, level++, dependedTypes))
                            if (!dependedTypes.Contains(innerDep)) {
                                yield return (innerDep);
                            }
                    }
                }
            }
        }

        public void Measure(IEnumerable<TypeDepScore> depscores) {
            if (this.Type.Name == "Exclusoes") {
                DepScore = -100000;
                IsMeasured = true;
                return;
            }
            if (IsMeasured) {
                return;
            }
            var matts = Type.AttributedMembersWhere<ForeignKeyAttribute>((m, fk) => true);
            foreach (var m in matts) {
                var fk = m.GetCustomAttribute<ForeignKeyAttribute>();
                foreach (var a in depscores) {
                    if (a == this || a.Type == Type || fk.RefType != a.Type) {
                        continue;
                    }
                    //EslCore.WriteLine($"{this.Type.Name} refs {fk.RefType}");
                    this.DepScore++;
                    a.Measure(depscores);
                    this.DepScore += a.DepScore;
                }
            }
            IsMeasured = true;
        }
    }
}
