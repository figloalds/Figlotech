using Figlotech.BDados.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Requirements {
    public interface IRequiresFileAccessor {
        [JsonIgnore]
        IFileAccessor FileAccessor { get; set; }
    }
}
