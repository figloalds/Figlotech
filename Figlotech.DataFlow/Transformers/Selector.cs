using Figlotech.DataFlow.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.DataFlow.Transformers
{
    public sealed class Selector : IDataSelector {
        public string OutputColumnName { get; private set; }
        private int DataIndex { get; set; } = -1;
        private string InputColumnName { get; set; }
        public Selector(int index, String outputName) {
            OutputColumnName = outputName;
            DataIndex = index;
        }
        public Selector(string inputName, String outputName) {
            OutputColumnName = outputName;
            InputColumnName = inputName;
        }
        public Selector(string passthroughName) {
            OutputColumnName = passthroughName;
            InputColumnName = passthroughName;
        }

        public object GetData(object[] input) {
            return DataIndex > -1 && DataIndex < input.Length ? input[DataIndex] : null;
        }

        public void SetupHeaders(string[] headers) {
            if(DataIndex == -1 && !string.IsNullOrEmpty(InputColumnName)) {
                DataIndex = headers.GetIndexOf(InputColumnName);
            } 
        }

        public void SetupInputTypes(Type[] types) {
        }
    }
}
