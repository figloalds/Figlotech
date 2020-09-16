using System;

namespace Figlotech.DataFlow.Models
{
    public interface IDataSelector
    {
        string OutputColumnName { get; }
        object GetData(object[] input);
        void SetupHeaders(string[] headers);
        void SetupInputTypes(Type[] types);
    }
}