using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Extensions
{
    public sealed class DoubleExtensionMapFromRangeHelper {
        double value; double low1; double high1;
        public DoubleExtensionMapFromRangeHelper(double value, double low1, double high1) {
            this.value = value;
            this.low1 = low1;
            this.high1 = high1;
        }
        public double To(double low2, double high2) {
            return (
                Math.Min(
                    Math.Max(
                        low2 + (value - low1) * (high2 - low2) / (high1 - low1), low2), 
                        high2
                    )
                );
        }
    }

    public static class DoubleExtensions {

        public static DoubleExtensionMapFromRangeHelper MapFrom(this double value, double low1, double high1) {
            return new DoubleExtensionMapFromRangeHelper(value, low1, high1);
        }
    }
}
