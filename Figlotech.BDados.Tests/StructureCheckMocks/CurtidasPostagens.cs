using AppRostoJovem.Backend.Models;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;

namespace AppRostoJovem.Backend.Models {
    public class CurtidasPostagens : DataObject<CurtidasPostagens> {

        [Field()]
        [ForeignKey(typeof(Usuarios))]
        public string Usuario;

        [Field()]
        [ForeignKey(typeof(Postagens))]
        public string Postagem;

    }
}
