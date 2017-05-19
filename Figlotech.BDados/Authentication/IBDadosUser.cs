using Figlotech.BDados.Attributes;
using Figlotech.BDados.Entity;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public interface IBDadosUser : IDataObject {
        String Username { get; set; }
        String Password { get; set; }
        bool isActive { get; set; }

        IBDadosPermissionsContainer GetPermissionsContainer();
    }
}
