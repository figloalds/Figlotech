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
// Tabela sessao 
// ------------------------------------------
namespace AppRostoJovem.Backend.Models {
    public enum TiposAcessoSessao { Interno, Social }
    public class SessoesUsuario :AppDataObject, IUserSession {

        [Field()]
        [ForeignKey(typeof(Usuarios))]
        public string Usuario;

        [Field()]
        public TiposAcessoSessao TipoAcesso;

        [Field(Size = 128)]
        public String Token;

        [Field(Size = 128, AllowNull = true)]
        public String Ip;

        [Field()]
        public DateTime Inicio;

        [Field(AllowNull = true)]
        public DateTime? Fim;

        [Field()]
        public bool Persistente;

        [Field()]
        public bool Ativo = true;

        [AggregateField(typeof(Usuarios), "Usuario", "NomeUsuario")]
        public string NomeUsuario;

        [AggregateField(typeof(Usuarios), "Usuario", "TipoUsuario")]
        public string TipoUsuario;
        
        public string User { get => Usuario; set => Usuario = value; }
        public bool isActive { get => Ativo; set => Ativo = value; }
        public DateTime StartTime { get => Inicio; set => Inicio = value; }
        public DateTime? EndTime { get => Fim; set => Fim = value; }
        
        public IPermissionsContainer Permission { get; set; }

        string IUserSession.Token { get => Token; set => Token = value; }
    }
}
