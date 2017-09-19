// --------------------------------------------------
// BDados v1.0.5822.25349
// Arquivo gerado automaticamente.
// --------------------------------------------------
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Diagnostics;

// ------------------------------------------
// Tabela pessoa
// ------------------------------------------
namespace AppRostoJovem.Backend.Models
{
    public class Pessoas : DataObject<Pessoas> {
        //
        // Declarações Globais
        // Valores do Objeto
        //
        [QueryComparison(DataStringComparisonType.Containing)]
        [Field(Size = 90, AllowNull = true)]
        public String RazaoSocial;

        [QueryComparison(DataStringComparisonType.Containing)]
        [Field(Size = 90)]
        public String Nome;

        [Field(DefaultValue = "1")]
        public int TipoPessoa;

        [QueryComparison(DataStringComparisonType.Containing)]
        [Field(Size = 45, AllowNull = true)]
        public String DocCadastro;

        [QueryComparison(DataStringComparisonType.Containing)]
        [Field(Size = 45, AllowNull = true)]
        public String DocInscricao;

        [Field(Size = 100, AllowNull = true)]
        public String Endereco;

        [Field(Size = 100, AllowNull = true)]
        public String Numero;

        [Field(Size = 45, AllowNull = true)]
        public String Bairro;

        [Field(Size = 45, AllowNull = true)]
        public String Cidade;

        [Field(Size = 4, AllowNull = true)]
        public String UF;

        [Field(Size = 64, AllowNull = true)]
        public String Cep;

        [Field(Size = 25, AllowNull = true)]
        public String Telefone;

        [Field(Size = 25, AllowNull = true)]
        public String Fax;

        [Field(Size = 75, AllowNull = true)]
        public String Email;

        [Field(Size = 45, AllowNull = true)]
        public String Site;

        [Field(Size = 500, AllowNull = true)]
        public String Observacoes;

        [Field(DefaultValue = false)]
        public bool Bloqueado = false;

        [Field(Type = "TEXT", AllowNull = true)]
        public String MotivoBloqueio;

        [Field(DefaultValue = true)]
        public bool Ativo = true;

        [Field(AllowNull = true)]
        public DateTime? DataInativacao;

        [Field(AllowNull = true)]
        public DateTime? DataNascimento;

        //
        // Declara��es Globais
        // Valores agregados
        //

    }
}
