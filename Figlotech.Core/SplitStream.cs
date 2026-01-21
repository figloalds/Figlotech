using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    /// <summary>
    /// SplitStream allows a single input Stream to be read by multiple consumers (forks) independently.
    /// It buffers the data from the source stream into blocks so that each fork can read at its own pace.
    /// It automatically discards blocks that have been read by all active forks to save memory.
    /// </summary>
    public class SplitStream : IDisposable
    {
        private readonly Stream _source;
        private readonly List<byte[]> _blocks = new List<byte[]>();
        private readonly List<TaskCompletionSource<bool>> _signals = new List<TaskCompletionSource<bool>>();
        private readonly List<SplitStreamView> _activeViews = new List<SplitStreamView>();
        private bool _isEndOfSource = false;
        private bool _hasStarted = false;
        private readonly SemaphoreSlim _readLock = new SemaphoreSlim(1, 1);
        private readonly object _syncLock = new object();
        private bool _isDisposed = false;
        private readonly bool _leaveOpen;
        private readonly int _maxCachedBlocks;
        private int _minRequestedIndex = 0;
        private TaskCompletionSource<bool> _throttleSignal = null;

        private const int DefaultBlockSize = 81920;
        public const int DefaultMaxCachedBlocks = 128; // ~10MB with default block size

        public SplitStream(Stream source, int? maxCachedBlocks = null, bool leaveOpen = false) {
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _maxCachedBlocks = maxCachedBlocks ?? DefaultMaxCachedBlocks;
            _leaveOpen = leaveOpen;
            // Signal for the potential first block
            _signals.Add(new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
        }

        /// <summary>
        /// Creates a new read-only view of the stream starting from the beginning.
        /// Once any fork starts reading, calling Fork() will throw an InvalidOperationException.
        /// </summary>
        /// <returns>A new Stream instance that reads from the same source.</returns>
        public Stream Fork() {
            lock (_syncLock) {
                if (_isDisposed) throw new ObjectDisposedException(nameof(SplitStream));
                if (_hasStarted) throw new InvalidOperationException("Cannot fork SplitStream after reading has started.");
                var view = new SplitStreamView(this);
                _activeViews.Add(view);
                return view;
            }
        }

        internal void UnregisterView(SplitStreamView view) {
            lock (_syncLock) {
                _activeViews.Remove(view);
                UpdateMinIndex();
            }
        }

        internal void UpdateMinIndex() {
            lock (_syncLock) {
                if (_activeViews.Count == 0) return;
                int newMin = _activeViews.Min(v => v.CurrentBlockIndex);
                if (newMin > _minRequestedIndex) {
                    for (int i = _minRequestedIndex; i < newMin; i++) {
                        if (i < _blocks.Count) {
                            _blocks[i] = null;
                        }
                    }
                    _minRequestedIndex = newMin;
                    // Release throttle if we were waiting
                    _throttleSignal?.TrySetResult(true);
                }
            }
        }

        internal async ValueTask<(byte[] block, bool eof)> GetBlockAsync(int index, CancellationToken cancellationToken) {
            TaskCompletionSource<bool> signal;
            lock (_syncLock) {
                if (_isDisposed) throw new ObjectDisposedException(nameof(SplitStream));
                _hasStarted = true;
                
                if (index < _minRequestedIndex) {
                    throw new InvalidOperationException("This part of the stream has already been discarded because all forks moved past it.");
                }
                if (index < _blocks.Count) return (_blocks[index], false);
                if (_isEndOfSource) return (null, true);
                
                while (_signals.Count <= index) {
                    _signals.Add(new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
                }
                signal = _signals[index];
            }

            // Throttling logic: if we are trying to read too far ahead of the slowest consumer
            while (index - _minRequestedIndex >= _maxCachedBlocks) {
                TaskCompletionSource<bool> waitSignal;
                lock (_syncLock) {
                    if (_throttleSignal == null || _throttleSignal.Task.IsCompleted) {
                        _throttleSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    }
                    waitSignal = _throttleSignal;
                }
                await waitSignal.Task.ConfigureAwait(false);
            }

            // Try to be the one who reads the block from the source
            if (await _readLock.WaitAsync(0, cancellationToken).ConfigureAwait(false)) {
                try {
                    // Double check after acquiring lock
                    lock (_syncLock) {
                        if (index < _blocks.Count) return (_blocks[index], false);
                        if (_isEndOfSource) return (null, true);
                    }

                    byte[] buffer = new byte[DefaultBlockSize];
                    int bytesRead = await _source.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);

                    lock (_syncLock) {
                        if (bytesRead > 0) {
                            if (bytesRead < buffer.Length) {
                                var actualBuffer = new byte[bytesRead];
                                Buffer.BlockCopy(buffer, 0, actualBuffer, 0, bytesRead);
                                buffer = actualBuffer;
                            }
                            _blocks.Add(buffer);
                            // Prepare signal for the next block
                            _signals.Add(new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
                        } else {
                            _isEndOfSource = true;
                        }
                        signal.TrySetResult(true);
                    }
                    
                    if (_isEndOfSource && bytesRead == 0) return (null, true);
                    return (buffer, false);
                } catch (Exception ex) {
                    signal.TrySetException(ex);
                    throw;
                } finally {
                    _readLock.Release();
                }
            } else {
                // Someone else is reading, wait for the signal
                try {
                    await signal.Task.ConfigureAwait(false);
                } catch (OperationCanceledException) {
                    throw;
                }
                
                lock (_syncLock) {
                    if (index < _blocks.Count) return (_blocks[index], false);
                    return (null, true);
                }
            }
        }

        public void Dispose() {
            lock (_syncLock) {
                if (_isDisposed) return;
                _isDisposed = true;
                _isEndOfSource = true;
                foreach (var signal in _signals) {
                    signal.TrySetResult(false);
                }
                _throttleSignal?.TrySetResult(false);
            }
            _readLock.Dispose();
            if (!_leaveOpen) {
                _source.Dispose();
            }
        }
    }

    /// <summary>
    /// A read-only Stream view into a SplitStream.
    /// Each view maintains its own cursor position and reads from the shared buffered blocks.
    /// </summary>
    public class SplitStreamView : Stream
    {
        private readonly SplitStream _parent;
        private int _blockIndex = 0;
        private int _blockOffset = 0;
        private long _position = 0;
        private bool _isDisposed = false;

        internal int CurrentBlockIndex => _blockIndex;

        public SplitStreamView(SplitStream parent) {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public override bool CanRead => !_isDisposed;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException("SplitStreamView does not support Length as the source might be ongoing.");
        public override long Position {
            get => _position;
            set => throw new NotSupportedException("SplitStreamView does not support setting Position.");
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
            if (_isDisposed) throw new ObjectDisposedException(nameof(SplitStreamView));
            
            var (block, eof) = await _parent.GetBlockAsync(_blockIndex, cancellationToken).ConfigureAwait(false);
            if (eof) return 0;

            int available = block.Length - _blockOffset;
            int toCopy = Math.Min(available, count);
            Buffer.BlockCopy(block, _blockOffset, buffer, offset, toCopy);

            _blockOffset += toCopy;
            _position += toCopy;

            if (_blockOffset >= block.Length) {
                _blockIndex++;
                _blockOffset = 0;
                _parent.UpdateMinIndex();
            }

            return toCopy;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
            if (_isDisposed) throw new ObjectDisposedException(nameof(SplitStreamView));

            var (block, eof) = await _parent.GetBlockAsync(_blockIndex, cancellationToken).ConfigureAwait(false);
            if (eof) return 0;

            int available = block.Length - _blockOffset;
            int toCopy = Math.Min(available, buffer.Length);
            
            block.AsMemory(_blockOffset, toCopy).CopyTo(buffer);

            _blockOffset += toCopy;
            _position += toCopy;

            if (_blockOffset >= block.Length) {
                _blockIndex++;
                _blockOffset = 0;
                _parent.UpdateMinIndex();
            }

            return toCopy;
        }

        public override int Read(byte[] buffer, int offset, int count) {
            // Synchronous read over async implementation
            return ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
        }

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing) {
            if (!_isDisposed) {
                _isDisposed = true;
                _parent.UnregisterView(this);
            }
            base.Dispose(disposing);
        }
    }
}
