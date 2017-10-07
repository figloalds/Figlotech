// --------------------------------------------------
// BDados v1.0.5822.25349
// Arquivo gerado automaticamente.
// --------------------------------------------------
using AppRostoJovem.WebCore;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Diagnostics;

// ------------------------------------------
// Tabela pessoa
// ------------------------------------------
namespace AppRostoJovem.Backend.Models {
    public class Grupos :AppDataObject {

        [Field(Size = 64, AllowNull = true)]
        [ForeignKey(typeof(Grupos))]
        public string GrupoPai { get; set; }

        [Field(Size = 500, AllowNull = true)]
        public string Descricao { get; set; }

        [Field(Size = 200, AllowNull = false)]
        public string Nome { get; set; }

        [Field(Size = 45, AllowNull = true)]
        public string Telefone { get; set; }

        [Field(Size = 80, AllowNull = true)]
        public string Email { get; set; }

        [Field(Size = 80, AllowNull = true)]
        public string FotoPerfil { get; set; }

        [Field(Size = 80, AllowNull = true)]
        public string FotoCapa { get; set; }


        [Field(Size = 64)]
        [ForeignKey(typeof(Niveis))]
        public string Nivel { get; set; }


        [Field(DefaultValue = true)]
        public bool ModerarPostagens { get; set; } = true;

        [Field(DefaultValue = false)]
        public bool GrupoAtivo { get; set; } = false;

        [AggregateField(typeof(Niveis), "Nivel", "Descricao")]
        public string NomeNivel { get; set; }

        [AggregateField(typeof(Grupos), "GrupoPai", "Nome")]
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
