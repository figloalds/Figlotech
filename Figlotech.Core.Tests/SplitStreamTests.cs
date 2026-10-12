using System.Collections;
using Figlotech.Core;
using Xunit;

namespace Figlotech.Core.Tests {
    public class SplitStreamTests {
        [Fact]
        public async Task SlowForkBackpressureIsCancelableAndDisposalReleasesIt() {
            using var input = new MemoryStream(new byte[81920 * 3]);
            using var split = new SplitStream(input, 1, true);
            using var fast = split.Fork();
            using var slow = split.Fork();
            var buffer = new byte[81920];
            Assert.Equal(buffer.Length, await fast.ReadAsync(buffer));
            using var cancellation = new CancellationTokenSource();
            var pending = fast.ReadAsync(buffer.AsMemory(), cancellation.Token).AsTask();
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(TimeSpan.FromSeconds(5)));
            slow.Dispose();
            Assert.Equal(buffer.Length, await fast.ReadAsync(buffer).AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        }

        [Fact]
        public async Task MultipleForksKeepBothDataAndMetadataBounded() {
            using var input = new PatternStream(81920L * 1000 + 17);
            using var split = new SplitStream(input, 2, true);
            using var first = split.Fork();
            using var second = split.Fork();
            var cache = (IDictionary)typeof(SplitStream).GetField("_blocks", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(split)!;
            async Task Consume(Stream fork, int size) {
                byte[] buffer = new byte[size];
                long offset = 0;
                int count;
                while ((count = await fork.ReadAsync(buffer)) != 0) {
                    Assert.Equal((byte)(offset % 251), buffer[0]);
                    Assert.Equal((byte)((offset + count - 1) % 251), buffer[count - 1]);
                    offset += count;
                }
                Assert.Equal(input.Length, offset);
            }
            await Task.WhenAll(Consume(first, 16384), Consume(second, 1023)).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Empty(cache);
            Assert.True(input.CanRead);
        }

        [Fact]
        public async Task SourceFailureReachesAllForksAndZeroReadDoesNotStart() {
            using var split = new SplitStream(new FailingStream(), 1);
            using var first = split.Fork();
            Assert.Equal(0, await first.ReadAsync(Memory<byte>.Empty));
            using var second = split.Fork();
            await Assert.ThrowsAsync<IOException>(() => first.ReadAsync(new byte[1]).AsTask());
            await Assert.ThrowsAsync<IOException>(() => second.ReadAsync(new byte[1]).AsTask());
            Assert.Throws<InvalidOperationException>(() => split.Fork());
        }

        [Fact]
        public async Task DisposeWakesThrottledForkAndDoesNotRaceReaderLock() {
            using var split = new SplitStream(new MemoryStream(new byte[81920 * 3]), 1);
            using var fast = split.Fork(); using var slow = split.Fork();
            Assert.Equal(81920, await fast.ReadAsync(new byte[81920]));
            var pending = fast.ReadAsync(new byte[1]).AsTask();
            split.Dispose();
            await Assert.ThrowsAsync<ObjectDisposedException>(() => pending.WaitAsync(TimeSpan.FromSeconds(5)));
            using var source = new WaitingStream();
            using var active = new SplitStream(source, 1, true);
            using var view = active.Fork();
            var reading = view.ReadAsync(new byte[1]).AsTask();
            active.Dispose(); source.Release.TrySetResult();
            await Assert.ThrowsAsync<ObjectDisposedException>(() => reading.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        [Theory]
        [InlineData(0)] [InlineData(-1)]
        public void InvalidCapacityFails(int capacity) => Assert.Throws<ArgumentOutOfRangeException>(() => new SplitStream(Stream.Null, capacity));

        private sealed class FailingStream : MemoryStream {
            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => ValueTask.FromException<int>(new IOException("synthetic"));
        }
        private sealed class WaitingStream : MemoryStream {
            internal TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) { await Release.Task.WaitAsync(cancellationToken); return 0; }
        }
        private sealed class PatternStream(long length) : Stream {
            private long _position;
            public override long Length => length;
            public override long Position { get => _position; set => throw new NotSupportedException(); }
            public override bool CanRead => true;
            public override bool CanWrite => false;
            public override bool CanSeek => false;
            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
                int count = (int)Math.Min(buffer.Length, length - _position);
                for (int i = 0; i < count; i++) buffer.Span[i] = (byte)((_position + i) % 251);
                _position += count; return ValueTask.FromResult(count);
            }
            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override void Flush() { }
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }
    }
}
