using Figlotech.BDados.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Requirements {
    public interface IRequiresDataAccessor {
        [JsonIgnore]
        IDataAccessor DataAccessor { get; set; }
    }
}
