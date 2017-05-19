using Figlotech.BDados.Attributes;
using Figlotech.BDados.Entity;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Authentication {
    public interface IBDadosUserSession : IDataObject {
        String User { get; set; }
        String Token { get; set; }
        bool isActive { get; set; }
        DateTime StartTime { get; set; }
        DateTime? EndTime { get; set; }

        IBDadosPermission Permission { get; set; }
    }
}
