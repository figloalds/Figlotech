// --------------------------------------------------
// BDados v1.0.5822.25349
// Arquivo gerado automaticamente.
// --------------------------------------------------
using System;
using Figlotech.BDados.Authentication;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using AppRostoJovem.WebCore;

// ------------------------------------------
// Tabela Permissoes
// ------------------------------------------
namespace AppRostoJovem.Backend.Models {
    public class Permissoes :AppDataObject {

        [Field( Size = 64)]
        public string Usuario;

        [Field(Size = 60)]
        public string Permissao;
    }
}
