using System;

namespace Figlotech.Core;

internal sealed class TimerCachedObject<T> {
    public T Object { get; set; }
    public DateTime LastChecked { get; set; }

    public TimerCachedObject(T objValue) {
        this.Object = objValue;
        this.LastChecked = DateTime.UtcNow;
    }

    public void KeepAlive() {
        LastChecked = DateTime.UtcNow;
    }
}
