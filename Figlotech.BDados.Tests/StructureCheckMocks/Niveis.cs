// --------------------------------------------------
// BDados v1.0.5822.25349
// Arquivo gerado automaticamente.
// --------------------------------------------------
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;

// ------------------------------------------
// Tabela pessoa
// ------------------------------------------
namespace AppRostoJovem.Backend.Models {
    public class Niveis : DataObject<Grupos> {

        [ForeignKey(typeof(Niveis))]
        [Field(Size = 64, AllowNull = true)]
        public string NivelPai { get; set; }

        [Field(Size = 200)]
        public string Descricao { get; set; }

        [AggregateField(typeof(Niveis), "NivelPai", "Descricao")]
        public string NomePai { get; set; }

        public QueryBuilder AcharHierarquia(long IdNivel) {
            return new QueryBuilder(@"
                SELECT  @r AS _rid,
                (
	       		    SELECT  @r := NivelPai
	       		    FROM    Niveis
         		    WHERE   RID = _rid
                ) AS NivelPai,
                @l := @l + 1 AS lvl
                FROM (
                    SELECT  @r := "+new { Nivel = IdNivel } +@",
                            @l := 0,
                            @cl := 0
                    ) vars,
                    Niveis h
                WHERE @r IS NOT NULL
            ");
        }

    }
}
