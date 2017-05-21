using Figlotech.BDados.Attributes;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public interface IUserSession : IDataObject {
        String User { get; set; }
        String Token { get; set; }
        bool isActive { get; set; }
        DateTime StartTime { get; set; }
        DateTime? EndTime { get; set; }

        IPermissionsContainer Permission { get; set; }
    }
}
