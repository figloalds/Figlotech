using AppRostoJovem.Backend.Models;
using AppRostoJovem.WebCore;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;

namespace AppRostoJovem.Backend.Models {
    public class CurtidasPostagens :AppDataObject {

        [Field()]
        [ForeignKey(typeof(Usuarios))]
        public string Usuario;

        [Field()]
        [ForeignKey(typeof(Postagens))]
        public string Postagem;

    }
}
