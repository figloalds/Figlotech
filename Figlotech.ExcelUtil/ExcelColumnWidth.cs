using System;

namespace Figlotech.ExcelUtil {
    internal static class ExcelColumnWidth {
        private const double DefaultMinimumWidth = 9.140625;
        private const double MaximumWidth = 255;

        internal static double NormalizeDpi(double width, double minimumWidth,
            double fontWidth, float dpi, bool hasAutoFilter) {
            minimumWidth = double.IsFinite(minimumWidth) && minimumWidth > 0
                ? Math.Clamp(minimumWidth, 1d, MaximumWidth) : DefaultMinimumWidth;
            if (!double.IsFinite(width) || width <= 0) {
                return minimumWidth;
            }
            var unscaledWidth = Math.Clamp(width, minimumWidth, MaximumWidth);
            // Unknown DPI/font metrics must never turn a usable EPPlus width into
            // NaN or Infinity: EPPlus will serialize those values without checking.
            if (!float.IsFinite(dpi) || dpi <= 0 || !double.IsFinite(fontWidth) || fontWidth <= 0) {
                return unscaledWidth;
            }

            // Text scales with DPI; EPPlus's fixed five-pixel padding does not.
            var scale = 96d / dpi;
            var padding = 5d / fontWidth;
            var normalizedWidth = (width - padding) * scale + padding;
            // EPPlus reserves 2.25 character units for filter buttons. Keep that
            // space even when the widest cell is the filtered header. A body cell
            // may instead determine the width, so this reserve is conservative.
            if (hasAutoFilter) {
                normalizedWidth += 2.25d * Math.Max(0d, 1d - scale);
            }
            return double.IsFinite(normalizedWidth)
                ? Math.Clamp(normalizedWidth, minimumWidth, MaximumWidth) : unscaledWidth;
        }
    }
}
