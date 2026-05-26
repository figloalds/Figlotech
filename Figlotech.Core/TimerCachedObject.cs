using System;
using System.Collections.Generic;

namespace Figlotech.Core;

internal sealed class TimerCachedObject<TKey, T> {
    public TimeSpan CacheDuration { get; private set; }
    public T Object { get; set; }
    public DateTime LastChecked { get; set; }
    readonly TKey Key;
    readonly IDictionary<TKey, TimerCachedObject<TKey, T>> CacheSource;

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
        KeepAlive();
    }

    public void KeepAlive() {
        LastChecked = DateTime.UtcNow;
    }
}
