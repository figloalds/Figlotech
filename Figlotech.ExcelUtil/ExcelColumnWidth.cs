using System;

namespace Figlotech.ExcelUtil {
    internal static class ExcelColumnWidth {
        internal static double NormalizeDpi(double width, double minimumWidth,
            double fontWidth, float dpi, bool hasAutoFilter) {
            var scale = 96d / dpi;
            var padding = 5d / fontWidth;
            var normalizedWidth = (width - padding) * scale + padding;
            // EPPlus reserves 2.25 character units for filter buttons. Keep that
            // space even when the widest cell is the filtered header. A body cell
            // may instead determine the width, so this reserve is conservative.
            if (hasAutoFilter) {
                normalizedWidth += 2.25d * Math.Max(0d, 1d - scale);
            }
            return Math.Max(minimumWidth, normalizedWidth);
        }
    }
}
