using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Figlotech.Core;

namespace Figlotech.BDados.Helpers {
    // CRC-32/ISO-HDLC, reflected polynomial 0xEDB88320, init/final XOR FFFFFFFF.
    internal sealed class BackupCrc32 {
        private static readonly uint[] Table = MakeTable();
        private uint _state = uint.MaxValue;
        internal uint Value => ~_state;
        internal void Append(ReadOnlySpan<byte> bytes) {
            foreach (byte value in bytes) _state = Table[(_state ^ value) & 255] ^ (_state >> 8);
        }
        internal static uint Compute(ReadOnlySpan<byte> bytes) { var crc = new BackupCrc32(); crc.Append(bytes); return crc.Value; }
        private static uint[] MakeTable() {
            var table = new uint[256];
            for (uint i = 0; i < table.Length; i++) {
                uint value = i;
                for (int bit = 0; bit < 8; bit++) value = (value >> 1) ^ ((value & 1) == 0 ? 0 : 0xEDB88320u);
                table[i] = value;
            }
            return table;
        }
    }

    /// <summary>Non-owning bounded view; cannot read or seek beyond a validated payload.</summary>
    internal sealed class BackupRangeStream : Stream {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Non-owning range; the archive API caller owns and disposes the source.")]
        private readonly Stream _source;
        private readonly long _start, _length;
        private long _position;
        internal BackupRangeStream(Stream source, long start, long length) { _source = source; _start = start; _length = length; source.Position = start; }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _length;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
            int count = (int)Math.Min(buffer.Length, _length - _position);
            if (count == 0) return 0;
            // The owner serializes all access to the underlying seekable stream.
            _source.Position = _start + _position;
            int read = await _source.ReadAsync(buffer[..count], cancellationToken);
            if (read == 0) throw new EndOfStreamException("Truncated archive payload.");
            _position += read;
            return read;
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    // Four 64 KiB producer chunks plus four SplitStream blocks bound the fan-out buffer.
    internal sealed class BackupChannelStream : Stream {
        private readonly Channel<byte[]> _channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(4) {
            SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait
        });
        private byte[] _current;
        private int _offset;
        private byte[] _writeBuffer = new byte[65536];
        private int _writeOffset;
        internal void Complete(Exception exception = null) => _channel.Writer.TryComplete(exception);
        private async Task FinishAsync(CancellationToken token) {
            if (_writeOffset != 0) await _channel.Writer.WriteAsync(_writeBuffer.AsSpan(0, _writeOffset).ToArray(), token);
            Complete();
        }
        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) {
            cancellationToken.ThrowIfCancellationRequested();
            while (!buffer.IsEmpty) {
                int count = Math.Min(buffer.Length, _writeBuffer.Length - _writeOffset);
                buffer.Span[..count].CopyTo(_writeBuffer.AsSpan(_writeOffset));
                _writeOffset += count;
                buffer = buffer[count..];
                if (_writeOffset == _writeBuffer.Length) {
                    await _channel.Writer.WriteAsync(_writeBuffer, cancellationToken);
                    _writeBuffer = new byte[65536];
                    _writeOffset = 0;
                }
            }
        }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
            if (buffer.IsEmpty) return 0;
            if (_current == null) {
                if (!await _channel.Reader.WaitToReadAsync(cancellationToken)) return 0;
                _current = await _channel.Reader.ReadAsync(cancellationToken);
                _offset = 0;
            }
            int count = Math.Min(buffer.Length, _current.Length - _offset);
            _current.AsMemory(_offset, count).CopyTo(buffer);
            _offset += count;
            if (_offset == _current.Length) _current = null;
            return count;
        }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        internal static async Task<uint> CopyWithCrcAsync(Stream destination, Func<Stream, CancellationToken, Task> produce, CancellationToken cancellationToken) {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using var channel = new BackupChannelStream();
            using var split = new SplitStream(channel, 4, leaveOpen: true);
            using var copy = split.Fork();
            using var checksum = split.Fork();
            var crc = new BackupCrc32();
            async Task Guard(Func<Task> action) {
                try { await action(); }
                catch (Exception exception) { channel.Complete(exception); linked.Cancel(); throw; }
            }
            async Task Produce() { await produce(channel, linked.Token); await channel.FinishAsync(linked.Token); }
            async Task Hash() {
                byte[] buffer = new byte[65536];
                int count;
                while ((count = await checksum.ReadAsync(buffer.AsMemory(), linked.Token)) != 0) crc.Append(buffer.AsSpan(0, count));
            }
            await Task.WhenAll(Guard(Produce), Guard(() => copy.CopyToAsync(destination, 65536, linked.Token)), Guard(Hash));
            return crc.Value;
        }
    }
}
