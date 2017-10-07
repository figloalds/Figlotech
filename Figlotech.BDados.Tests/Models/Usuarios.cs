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
// Tabela usuario 
// ------------------------------------------
namespace AppRostoJovem.Backend.Models {
    public class Usuarios :AppDataObject, IUser {

        [Field(Unique = true, Size = 60, AllowNull = true)]
        public string NomeUsuario;

        [Field(Size = 128, AllowNull = true)]
        public string Senha;

        [Field(AllowNull=true)]
		public DateTime? UltimoLogin;

		[Field(Size=100, AllowNull=true)]
		public String Email;
        
		[Field(DefaultValue="0")]
		public bool EmailCfm;

		[Field(Size=256, AllowNull=true)]
		public String TokenCfmEmail;
        
        [Field(DefaultValue = false)]
        public bool Ativo = true;

        [Field(Size = 45, DefaultValue = "Normal")]
        public string TipoUsuario;


        //[AggregateField(typeof(Pessoas), "Pessoa", "Nome")]
        //public String NomePessoa;

        [AggregateList(typeof(Permissoes), "Usuario")]
        public RecordSet<Permissoes> Permissoes;


        public string Username { get => NomeUsuario; set => NomeUsuario = value; }
        public string Password { get => Senha; set => Senha = value; }
        public bool isActive { get => Ativo; set => Ativo = value; }

    }
}
