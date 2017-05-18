using Figlotech.BDados.Attributes;
using Figlotech.BDados.Entity;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public enum BDadosPermissions {
        Create      = 0b1,
        Read        = 0b10,
        Update      = 0b100,        
        Delete      = 0b1000,
        Authorize   = 0b10000,
    }
    public interface IBDadosPermission : IDataObject {
        String User { get; set; }
        String Module { get; set; }
        String Resource { get; set; }
        /// <summary>
        /// This is supposed to be a sum of BDadosPermissions
        /// It's a C-Like additive tag 
        /// </summary>
        int Permission { get; set; }
    }
}
