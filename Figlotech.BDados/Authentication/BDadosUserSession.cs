using Figlotech.BDados.DataAccessAbstractions;
using System;

namespace Figlotech.BDados.Authentication
{
    public interface IUserSession : IDataObject {
        String User { get; set; }
        String Token { get; set; }
        bool isActive { get; set; }
        DateTime StartTime { get; set; }
        DateTime? EndTime { get; set; }
    }
}
