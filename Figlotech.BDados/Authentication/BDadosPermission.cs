using Figlotech.BDados.Attributes;
using Figlotech.BDados.Entity;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public interface IBDadosPermission : IDataObject {
        String User { get; set; }
        String Module { get; set; }
        String Resource { get; set; }
        bool CanCreate { get; set; }
        bool CanRead { get; set; }
        bool CanUpdate { get; set; }
        bool CanDelete { get; set; }
        bool CanAuthorize { get; set; }
    }
}
