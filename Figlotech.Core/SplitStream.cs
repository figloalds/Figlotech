using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    /// <summary>Bounded fan-out of a readable stream. Create all forks before reading; each fork has one reader.</summary>
    public class SplitStream : IDisposable {
        private readonly Stream _source;
        private readonly bool _leaveOpen;
        private readonly int _maxCachedBlocks;
        private readonly object _syncLock = new object();
        private readonly Dictionary<long, byte[]> _blocks = new Dictionary<long, byte[]>();
        private readonly List<SplitStreamView> _views = new List<SplitStreamView>();
        private TaskCompletionSource<bool> _changed = NewSignal();
        private ExceptionDispatchInfo _failure;
        private long _nextIndex;
        private long _firstIndex;
        private bool _reading, _started, _ended, _disposed;
        private const int BlockSize = 81920;
        public const int DefaultMaxCachedBlocks = 128;

        public SplitStream(Stream source, int? maxCachedBlocks = null, bool leaveOpen = false) {
            _source = source ?? throw new ArgumentNullException(nameof(source));
            if (!source.CanRead) throw new ArgumentException("Source must be readable.", nameof(source));
            _maxCachedBlocks = maxCachedBlocks ?? DefaultMaxCachedBlocks;
            if (_maxCachedBlocks <= 0) throw new ArgumentOutOfRangeException(nameof(maxCachedBlocks));
            _leaveOpen = leaveOpen;
        }

        private static TaskCompletionSource<bool> NewSignal() => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        private void Pulse() { var old = _changed; _changed = NewSignal(); old.TrySetResult(true); }

        /// <summary>Creates a fork before any fork begins reading.</summary>
        public Stream Fork() => new SplitStreamView(this);

        internal void Register(SplitStreamView view) {
            lock (_syncLock) {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (_started) throw new InvalidOperationException("Cannot fork SplitStream after reading has started.");
                _views.Add(view);
            }
        }

        internal void UnregisterView(SplitStreamView view) {
            lock (_syncLock) { _views.Remove(view); UpdateMinIndex(); }
        }

        internal void UpdateMinIndex() {
            lock (_syncLock) {
                long minimum = _views.Count == 0 ? _nextIndex : _views.Min(v => v.CurrentBlockIndex);
                while (_firstIndex < minimum) _blocks.Remove(_firstIndex++);
                Pulse();
            }
        }

        internal async ValueTask<byte[]> GetBlockAsync(long index, CancellationToken cancellationToken) {
            while (true) {
                cancellationToken.ThrowIfCancellationRequested();
                Task wait;
                lock (_syncLock) {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    _started = true;
                    _failure?.Throw();
                    if (index < _firstIndex) throw new InvalidOperationException("Fork has been disposed or its data discarded.");
                    if (_blocks.TryGetValue(index, out var block)) return block;
                    if (_ended) return null;
                    wait = _changed.Task;
                    if (!_reading && _blocks.Count < _maxCachedBlocks) { _reading = true; wait = null; }
                }
                if (wait != null) { await wait.WaitAsync(cancellationToken).ConfigureAwait(false); continue; }
                try {
                    byte[] buffer = new byte[BlockSize];
                    int count = await _source.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                    lock (_syncLock) {
                        ObjectDisposedException.ThrowIf(_disposed, this);
                        if (count == 0) _ended = true;
                        else {
                            if (count != buffer.Length) Array.Resize(ref buffer, count);
                            _blocks.Add(_nextIndex++, buffer);
                        }
                    }
                } catch (Exception exception) {
                    lock (_syncLock) _failure = ExceptionDispatchInfo.Capture(exception);
                    throw;
                } finally {
                    lock (_syncLock) { _reading = false; Pulse(); }
                }
            }
        }

        public void Dispose() {
            lock (_syncLock) {
                if (_disposed) return;
                _disposed = true;
                _blocks.Clear();
                Pulse();
            }
            if (!_leaveOpen) _source.Dispose();
        }
    }

    /// <summary>Single-reader, forward-only view owned by its caller.</summary>
    public class SplitStreamView : Stream {
        private readonly SplitStream _parent;
        private long _blockIndex, _position;
        private int _blockOffset;
        private bool _disposed;
        internal long CurrentBlockIndex => Interlocked.Read(ref _blockIndex);
        public SplitStreamView(SplitStream parent) { _parent = parent ?? throw new ArgumentNullException(nameof(parent)); _parent.Register(this); }
        public override bool CanRead => !_disposed;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();
            if (buffer.IsEmpty) return 0;
            byte[] block = await _parent.GetBlockAsync(_blockIndex, cancellationToken).ConfigureAwait(false);
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (block == null) return 0;
            int count = Math.Min(buffer.Length, block.Length - _blockOffset);
            block.AsMemory(_blockOffset, count).CopyTo(buffer);
            _blockOffset += count;
            _position += count;
            if (_blockOffset == block.Length) { Interlocked.Increment(ref _blockIndex); _blockOffset = 0; _parent.UpdateMinIndex(); }
            return count;
        }
        public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        protected override void Dispose(bool disposing) {
            if (!_disposed) { _disposed = true; _parent.UnregisterView(this); }
            base.Dispose(disposing);
        }
    }
}
