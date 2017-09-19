// --------------------------------------------------
// BDados v1.0.5822.25349
// Arquivo gerado automaticamente.
// --------------------------------------------------
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;

// ------------------------------------------
// Tabela Permissoes
// ------------------------------------------
namespace AppRostoJovem.Backend.Models {
    public class Permissoes : DataObject<Permissoes> {

        [Field( Size = 64)]
        public string Usuario;

        [Field(Size = 60)]
        public string Permissao;
    }
}
