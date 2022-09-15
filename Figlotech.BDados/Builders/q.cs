/**
* Iaetec.BDados.Builders.QueryBuilder
* Default implementation for IQueryBuilder
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using Figlotech.BDados.Builders;
using Figlotech.Data;
using System;

namespace Figlotech.BDados.Builders {
    public sealed class q : QueryBuilder {
        public q() { }
        public q(String fragment, params object[] args) {
            Append(fragment, args);
        }
    }
}
