using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AppRostoJovem.Backend.Models {
    public class GruposUsuario : DataObject<GruposUsuario> {

        [Field(Size = 64)]
        [ForeignKey(typeof(Usuarios))]
        public string Usuario { get; set; }

        [Field(Size = 64)]
        [ForeignKey(typeof(Grupos))]
        public string Grupo { get; set; }



    }
}
