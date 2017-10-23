using System;

namespace Figlotech.Core.Helpers
{
    public interface IBenchmarker {
        bool WriteToStdout { get; set; }

        double Mark(String txt);
        double TotalMark();
    }
}
