using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.CustomForms.FetchScript {
    public interface IFetchScriptBlockStatement {
        bool Init(Object[] args);
        bool ExecuteBlock(List<String> block);
    }
}
