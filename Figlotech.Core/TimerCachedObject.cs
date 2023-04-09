using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Figlotech.Core;
internal sealed class TimerCachedObject<TKey, T> : IDisposable {
    public TimeSpan CacheDuration { get; private set; }
    public T Object { get; set; }
    public DateTime LastChecked { get; set; }
    TKey Key;
    Timer Timer;
    IDictionary<TKey, TimerCachedObject<TKey, T>> CacheSource;

    public bool IsDisposed { get; set; } = false;

    public TimerCachedObject(
        IDictionary<TKey, TimerCachedObject<TKey, T>> cacheSource,
        TKey key, T objValue,
        TimeSpan duration
    ) {
        this.CacheSource = cacheSource;
        this.Object = objValue;
        this.LastChecked = DateTime.UtcNow;
        this.CacheDuration = duration;
        this.Key = key;
        RefreshTimer();
    }

    public void RefreshTimer() {
        DateTime DataUpdate = DateTime.UtcNow;
        if (this.Timer != null) {
            this.Timer.Dispose();
        }
        var timeout = this.Object != null ? (int)CacheDuration.TotalMilliseconds : 0;
        this.Timer = new Timer((s) => {
            lock (CacheSource)
                if (CacheSource.ContainsKey(this.Key)) {
                    CacheSource.Remove(this.Key);
                }
            this.Dispose();
        }, null, timeout, Timeout.Infinite);
    }

    public void Dispose() {
        if (!this.IsDisposed) {
            try {
                this.Timer.Dispose();
            } catch (Exception x) { }
            try {
                if (this.Object is IDisposable d) {
                    d.Dispose();
                }
            } catch (Exception x) { }
            this.IsDisposed = true;
        }
    }

    ~TimerCachedObject() {
        this.Dispose();
    }
}
