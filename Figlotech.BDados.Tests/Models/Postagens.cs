using AppRostoJovem.WebCore;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;

namespace AppRostoJovem.Backend.Models {
    public class Postagens : AppDataObject {

        //
        // Declarações Globais
        // Valores do Objeto
        //

        [Field(Type = "TEXT", AllowNull = true)]
        public String Titulo;

        [Field(Type = "LONGTEXT")]
        public String Conteudo;

        [Field(Size = 256, AllowNull = true)]
        public String PermaLink;

        [Field(Size = 64)]
        [ForeignKey(typeof(Usuarios))]
        public string Autor;

        [Field(Size = 64)]
        [ForeignKey(typeof(Grupos))]
        public string Grupo;

        [Field(DefaultValue = true)]
        public bool PermitirComentarios;

        [Field(DefaultValue = true)]
        public bool ModerarComentarios;

        [Field(DefaultValue = true)]
        public int Situacao;

        [Field()]
        public DateTime DataPostagem;

        [Field(DefaultValue = false)]
        public bool Destaque;

        [Field(DefaultValue = false)]
        public int Curtidas;

        //
        // Declarações Globais
        // Valores agregados
        //
        

        public String Resumo;
        public String NomeAutor;

    }
}
