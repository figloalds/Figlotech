// --------------------------------------------------
// BDados v1.0.5822.25349
// Arquivo gerado automaticamente.
// --------------------------------------------------
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;

// ------------------------------------------
// Tabela pessoa
// ------------------------------------------
namespace AppRostoJovem.Backend.Models {
    public class Grupos : DataObject<Grupos> {

        [Field(Size = 64, AllowNull = true)]
        [ForeignKey(typeof(Grupos))]
        public string GrupoPai { get; set; }

        [Field(Size = 200)]
        public string Descricao { get; set; }

        [Field(Size = 45)]
        public string Telefone { get; set; }

        [Field(Size = 80)]
        public string Email { get; set; }

        [Field(Size = 64)]
        [ForeignKey(typeof(Niveis))]
        public string Nivel { get; set; }

        [Field(DefaultValue = true)]
        public bool ModerarPostagens { get; set; } = true;

        [AggregateField(typeof(Niveis), "Nivel", "Descricao")]
        public string NomeNivel { get; set; }

        [AggregateField(typeof(Grupos), "GrupoPai", "Descricao")]
        public string NomePai { get; set; }

        public static QueryBuilder AcharHierarquia(String RidGrupo) {
            return new QueryBuilder(@"
                SELECT  @r AS _rid,
                (
	       		    SELECT  @r := GrupoPai
	       		    FROM    Grupos
         		    WHERE   RID = _rid
                ) AS GrupoPai,
                @l := @l + 1 AS lvl
                FROM (
                    SELECT  @r := " + new { Grupo = RidGrupo } + @",
                            @l := 0,
                            @cl := 0
                    ) vars,
                    Grupos h
                WHERE @r IS NOT NULL
            ");
        }

    }
}
