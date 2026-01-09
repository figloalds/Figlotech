
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using Figlotech.Core.Interfaces;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados {

    public class PersistenceCacheObject<T> where T : IDataObject, new() {
        public bool IsDirty { get; set; } = false;
        public DateTime LastSetDirty { get; set; } = DateTime.UtcNow;
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
        public T Object { get; set; }
    }

    public sealed class DelayedPersistenceCache<T> : IAsyncDisposable, IDictionary<string, T> where T : IDataObject, new() {
        private TimedCache<string, PersistenceCacheObject<T>> Dictionary;
        private TimeSpan CacheDuration { get; set; }
        private TimeSpan PersistenceInterval { get; set; }
        private IRdbmsDataAccessor DataAccessor { get; set; }

        public ICollection<string> Keys => Dictionary.Keys;

        public ICollection<T> Values => Dictionary.Values.Select(x=> x.Object).ToList();

        public Func<T, Task<T>> CustomOnLoad { get; set; }

        public int Count => Dictionary.Count;

        public bool IsReadOnly => Dictionary.IsReadOnly;

        private string SelfScheduleId = $"PersistenceCache{RID.GenerateNewAsBase36()}";

        public int MaxPersistenceBatchPerInterval = 100;

        public T this[string key] { 
            get => Dictionary.TryGetValue(key, out var val) ? val.Object : default(T); 
            set => Add(key, value); 
        }

        public DelayedPersistenceCache(
            IRdbmsDataAccessor dataAccessor, 
            TimeSpan cacheDuration, 
            TimeSpan persistenceInterval,
            WorkQueuer workQueuer = null
        ) {
            this.CacheDuration = cacheDuration;
            this.PersistenceInterval = persistenceInterval;
            this.DataAccessor = dataAccessor;
            Dictionary = new TimedCache<string, PersistenceCacheObject<T>>(cacheDuration);
            Dictionary.OnSet = async (key, value) => {
                await Task.Yield();
                if(!value.IsDirty) {
                    value.LastSetDirty = DateTime.UtcNow;
                    value.IsDirty = true;
                }
                value.LastUpdated = DateTime.UtcNow;
            };
            Dictionary.OnFree = async (key) => {
                var obj = Dictionary[key];
                if (obj.IsDirty) {
                    await SaveToPersistentStorage(obj.Object);
                }
            };
            Dictionary.OnFailOver = async (key) => {
                using var tsn = await DataAccessor.CreateNewTransactionAsync(CancellationToken.None);
                var obj = new PersistenceCacheObject<T> { 
                    Object = 
                        CustomOnLoad != null
                        ? await CustomOnLoad(await DataAccessor.LoadByRidAsync<T>(tsn, key))
                        : await DataAccessor.LoadByRidAsync<T>(tsn, key),
                };
                await SaveToPersistentStorage(obj.Object);
                return obj;
            };
            Fi.Tech.ScheduleTask(SelfScheduleId, workQueuer ?? FiTechCoreExtensions.GlobalQueuer, DateTime.UtcNow + persistenceInterval, new WorkJob(async () => {
                try {
                    await using var tsn = await DataAccessor.CreateNewTransactionAsync(CancellationToken.None);
                    var dirtyObjects = Dictionary.Values
                        .Where(x => x.IsDirty)
                        .OrderBy(x=> x.LastSetDirty)
                        .Take(MaxPersistenceBatchPerInterval)
                        .Select(x => x.Object)
                        .ToList();
                    if (dirtyObjects.Count > 0) {
                        await SaveToPersistentStorage(dirtyObjects);
                    }
                } catch (Exception ex) {
                    Debug.WriteLine($"Error during scheduled persistence: {ex.Message}");
                }
            }) { 
                Name = "DelayedPersistenceCache Persistence Task",
            }, persistenceInterval);
        }

        bool isDisposed = false;
        async ValueTask IAsyncDisposable.DisposeAsync() {
            if (isDisposed) return;
            isDisposed = true;
            // Save all dirty objects to persistent storage
            foreach (var key in Dictionary.Keys.ToList()) {
                var obj = Dictionary[key];
                if (obj.IsDirty) {
                    await SaveToPersistentStorage(obj.Object);
                }
            }
            // Dispose of the dictionary
            Dictionary.Clear();
        }

        public async Task<(bool Success, T Value)> TryGetValueAsync(string key) {
            var retv = await Dictionary.TryGetValueAsync(key);
            return (retv.Item1, retv.Item1 ? retv.Item2.Object : default(T));
        }

        public void PutClean(T value) {
            if (Dictionary.ContainsKey(value.RID)) {
                Dictionary[value.RID].Object = value;
                Dictionary[value.RID].IsDirty = false;
                Dictionary[value.RID].LastUpdated = DateTime.UtcNow;
            } else {
                Dictionary[value.RID] = new PersistenceCacheObject<T> { Object = value, IsDirty = false, LastUpdated = DateTime.UtcNow };
            }
        }

        public async Task Reset() {
            var dirtyValues = Dictionary.Values
                .Where(x => x.IsDirty)
                .Select(x => x.Object)
                .ToList();
            await SaveToPersistentStorage(dirtyValues);
            Dictionary.Clear();
        }

        private async Task SaveToPersistentStorage<T>(IEnumerable<T> obj) where T : IDataObject, new() {
            try {
                await using var tsn = await DataAccessor.CreateNewTransactionAsync(CancellationToken.None);
                await DataAccessor.SaveListAsync<T>(tsn, obj.ToList());
            } catch (Exception ex) {
                Debug.WriteLine($"Error persisting objects: {ex.Message}");
            }
        }

        private async Task SaveToPersistentStorage<T>(T obj) where T: IDataObject, new() {
            try {
                await using var tsn = await DataAccessor.CreateNewTransactionAsync(CancellationToken.None);
                await DataAccessor.SaveItemAsync(tsn, obj);
            } catch (Exception ex) {
                Debug.WriteLine($"Error object: {ex.Message}");
            }
        }

        public void Add(string key, T value) {
            if (Dictionary.ContainsKey(key)) {
                Dictionary[key].Object = value;
                Dictionary[key].IsDirty = true;
                Dictionary[key].LastUpdated = DateTime.UtcNow;
            } else {
                Dictionary[key] = new PersistenceCacheObject<T> { Object = value, IsDirty = true, LastUpdated = DateTime.UtcNow };
            }
        }

        public bool ContainsKey(string key) {
            return Dictionary.ContainsKey(key);
        }

        public bool Remove(string key) {
            if (Dictionary.ContainsKey(key)) {
                var obj = Dictionary[key];
                if (obj.IsDirty) {
                    SaveToPersistentStorage(obj.Object).ConfigureAwait(false);
                }
                return Dictionary.Remove(key);
            }
            return false;
        }

        public bool TryGetValue(string key, out T value) {
            if (Dictionary.TryGetValue(key, out var cacheObj)) {
                value = cacheObj.Object;
                cacheObj.IsDirty = true; // Mark as dirty since we're accessing it
                cacheObj.LastUpdated = DateTime.UtcNow;
                return true;
            }
            value = default(T);
            return false;
        }

        public void Add(KeyValuePair<string, T> item) {
            if (Dictionary.ContainsKey(item.Key)) {
                Dictionary[item.Key].Object = item.Value;
                Dictionary[item.Key].IsDirty = true;
                Dictionary[item.Key].LastUpdated = DateTime.UtcNow;
            } else {
                Dictionary[item.Key] = new PersistenceCacheObject<T> { Object = item.Value, IsDirty = true, LastUpdated = DateTime.UtcNow };
            }
        }

        public void Clear() {
            foreach (var key in Dictionary.Keys.ToList()) {
                if (Dictionary[key].IsDirty) {
                    SaveToPersistentStorage(Dictionary[key].Object).ConfigureAwait(false);
                }
            }
            Dictionary.Clear();
        }

        public bool Contains(KeyValuePair<string, T> item) {
            return Dictionary.ContainsKey(item.Key);
        }

        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex) {
            if (array == null) {
                throw new ArgumentNullException(nameof(array));
            }
            if (arrayIndex < 0 || arrayIndex >= array.Length) {
                throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            }
            if (array.Length - arrayIndex < Dictionary.Count) {
                throw new ArgumentException("Not enough space in the target array.");
            }
            foreach (var kvp in Dictionary) {
                array[arrayIndex++] = new KeyValuePair<string, T>(kvp.Key, kvp.Value.Object);
            }
        }

        public bool Remove(KeyValuePair<string, T> item) {
            if (Dictionary.ContainsKey(item.Key)) {
                var obj = Dictionary[item.Key];
                if (obj.IsDirty) {
                    SaveToPersistentStorage(obj.Object).ConfigureAwait(false);
                }
                return Dictionary.Remove(item.Key);
            }
            return false;
        }

        public IEnumerator<KeyValuePair<string, T>> GetEnumerator() {
            foreach (var kvp in Dictionary) {
                yield return new KeyValuePair<string, T>(kvp.Key, kvp.Value.Object);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }
    }
}
