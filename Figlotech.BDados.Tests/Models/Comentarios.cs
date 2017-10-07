using AppRostoJovem.Backend.Models;
using AppRostoJovem.WebCore;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace AppRostoJovem.Backend.Models {
    public class Comentarios : AppDataObject {

        [Field(Size = 64)]
        [ForeignKey(typeof(Usuarios))]
        public String Usuario;

        [QueryComparison(DataStringComparisonType.Containing)]
        [Field(Size = 200)]
        public String Titulo;

        [QueryComparison(DataStringComparisonType.Containing)]
        [Field(Type = "TEXT")]
        public String Conteudo;

        [Field()]
        public bool Aprovado;

        [Field(Size = 64)]
        [ForeignKey(typeof(Postagens))]
        public string Postagem;

        [Field(DefaultValue = 0)]
        public int Karma;
        
        public String NomeUsuario;
        public bool Curtiu = false;
        public int Curtidas = 0;

    }
}
