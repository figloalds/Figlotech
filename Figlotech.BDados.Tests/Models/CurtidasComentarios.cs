using AppRostoJovem.Backend.Models;
using AppRostoJovem.WebCore;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;

namespace AppRostoJovem.Backend.Models {
    public class CurtidasComentarios : AppDataObject {

        [Field(Size = 64)]
        [ForeignKey(typeof(Usuarios))]
        public string Usuario;

        [Field(Size = 64)]
        [ForeignKey(typeof(Comentarios))]
        public string Comentario;
    }
}
