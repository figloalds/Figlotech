using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    /// <summary>
    /// Bridges UTF-8 bytes from <see cref="System.Text.Json.Utf8JsonWriter"/> to a
    /// <see cref="TextWriter"/>. Utf8JsonWriter emits bytes; TextWriter consumes chars.
    /// This adapter decodes the UTF-8 byte chunks and forwards the decoded strings
    /// incrementally, preserving streaming semantics (no full-result buffering).
    /// </summary>
    internal sealed class TextWriterUtf8Stream : Stream {
        private readonly TextWriter _output;
        private readonly Decoder _decoder;
        private readonly char[] _charBuffer;

        public TextWriterUtf8Stream(TextWriter output) {
            _output = output ?? throw new ArgumentNullException(nameof(output));
            _decoder = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false).GetDecoder();
            _charBuffer = new char[4096];
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() {
            FlushDecoder();
            _output.Flush();
        }

        public override async Task FlushAsync(CancellationToken cancellationToken) {
            FlushDecoder();
            await _output.FlushAsync().ConfigureAwait(false);
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) {
            // Decode bytes incrementally; the decoder carries over incomplete multi-byte
            // sequences across Write calls until Flush() is called.
            int charsDecoded;
            int totalRead = 0;
            while (totalRead < count) {
                _decoder.Convert(buffer, offset + totalRead, count - totalRead,
                                 _charBuffer, 0, _charBuffer.Length,
                                 flush: false, out int bytesUsed, out charsDecoded, out _);
                if (charsDecoded > 0) {
                    _output.Write(_charBuffer, 0, charsDecoded);
                }
                totalRead += bytesUsed;
                if (bytesUsed == 0 && charsDecoded == 0)
                    break;
            }
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
            int totalRead = 0;
            while (totalRead < count) {
                _decoder.Convert(buffer, offset + totalRead, count - totalRead,
                                 _charBuffer, 0, _charBuffer.Length,
                                 flush: false, out int bytesUsed, out int charsDecoded, out _);
                if (charsDecoded > 0) {
                    await _output.WriteAsync(_charBuffer.AsMemory(0, charsDecoded), cancellationToken).ConfigureAwait(false);
                }
                totalRead += bytesUsed;
                if (bytesUsed == 0 && charsDecoded == 0)
                    break;
            }
        }

        private void FlushDecoder() {
            // Drain any remaining bytes held by the decoder.
            _decoder.Convert(Array.Empty<byte>(), 0, 0,
                             _charBuffer, 0, _charBuffer.Length,
                             flush: true, out _, out int charsDecoded, out _);
            if (charsDecoded > 0) {
                _output.Write(_charBuffer, 0, charsDecoded);
            }
        }

        protected override void Dispose(bool disposing) {
            if (disposing) {
                FlushDecoder();
            }
            base.Dispose(disposing);
        }
    }
}
