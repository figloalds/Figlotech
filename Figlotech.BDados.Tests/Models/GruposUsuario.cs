using AppRostoJovem.WebCore;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AppRostoJovem.Backend.Models {
    public class GruposUsuario :AppDataObject {

        [Field(Size = 64)]
        [ForeignKey(typeof(Usuarios))]
        public string Usuario { get; set; }

        [Field(Size = 64)]
        [ForeignKey(typeof(Grupos))]
        public string Grupo { get; set; }

        [Field(DefaultValue = false)]
        public bool Adm { get; set; } = false;

        [AggregateField(typeof(Usuarios), "Usuario", "TipoUsuario")]
        public string TipoUsuario;

        [AggregateObject("Grupo")]
        public Grupos ObjGrupo { get; set; }

    }
}
