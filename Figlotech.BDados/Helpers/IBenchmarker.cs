using System;

namespace Figlotech.BDados.Helpers
{
    public interface IBenchmarker {
        bool WriteToStdout { get; set; }

        double Mark(String txt);
        double TotalMark();
    }
}
