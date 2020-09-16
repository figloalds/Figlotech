using Figlotech.Core.Data;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.DataFlow.Interfaces;
using Figlotech.DataFlow.Models;
using Figlotech.DataFlow.Transformers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.DataFlow.Readers
{
    public sealed class SimpleSelect : DataTransform {

        Selector[] Selectors;

        public SimpleSelect(IEnumerable<Selector> Selectors) {
            this.Selectors = Selectors.ToArray();
        }

        ~SimpleSelect() {
            this.Dispose();
        }
        
        public override async Task<string[]> GetHeaders() {
            var headers = await Input.GetHeaders();
            var retv = new string[this.Selectors.Length];
            for(int i = 0; i < Selectors.Length; i++) {
                Selectors[i].SetupHeaders(headers);
                retv[i] = Selectors[i].OutputColumnName;
            }
            return retv;
        }

        public override object[] ProcessInput(object[] input) {
            var retv = new object[Selectors.Length];
            for (int i = 0; i < Selectors.Length; i++) {
                retv[i] = Selectors[i].GetData(input);
            }
            return retv;
        }
    }
}
