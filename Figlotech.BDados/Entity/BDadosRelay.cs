


using Figlotech.BDados.Attributes;
/**
* Figlotech.BDados.Builders.BDadosRElay
* (unnused)
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity {
    public enum OperacaoWrite {
        Insert = 1,
        Update = 2,
        Delete = 3,
    }

    public class BDadosRelay : DataObject<BDadosRelay> {

        [FieldAttribute(Size = 64)]
        public String Tabela;

        [FieldAttribute(Size = 64)]
        public String ObjRID;

        [FieldAttribute()]
        public DateTime Registro;

        [FieldAttribute()]
        public OperacaoWrite Operacao;
    }
}
