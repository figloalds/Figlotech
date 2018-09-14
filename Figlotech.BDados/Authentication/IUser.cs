using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core.Interfaces;
using System;

namespace Figlotech.BDados.Authentication
{
    public interface IUser : IDataObject {
        String Username { get; set; }
        String Password { get; set; }

        bool isActive { get; set; }
        
    }
}
