using System;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.Core.DomainEvents;
using Xunit;

namespace Figlotech.Core.Tests {

    public class TestEvent : DomainEvent {
        public TestEvent() : base() { }
    }

    public class OtherEvent : DomainEvent {
        public OtherEvent() : base() { }
    }

    public class DomainEventsHubTests {

        [Fact]
        public async Task Raise_InvokesMatchingListener() {
            var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
            var tcs = new TaskCompletionSource<TestEvent>();
            using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => {
                tcs.TrySetResult(e);
                return default(ValueTask);
            }))) {
                var evt = new TestEvent();
                hub.Raise(evt);
                var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Same(evt, result);
            }
        }

        [Fact]
        public async Task RaiseAndWaitForHandlers_AwaitsAllListeners() {
            var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
            int counter = 0;
            using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => {
                Interlocked.Increment(ref counter);
                return default(ValueTask);
            })))
            using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => {
                Interlocked.Increment(ref counter);
                return default(ValueTask);
            }))) {
                await hub.RaiseAndWaitForHandlers(new TestEvent());
                Assert.Equal(2, counter);
            }
        }

        [Fact]
        public async Task CatchAllListener_ReceivesAllEventTypes() {
            var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
            var tcs = new TaskCompletionSource<IDomainEvent>();
            using (hub.SubscribeListener(new CatchAllRelay(e => {
                tcs.TrySetResult(e);
                return default(ValueTask);
            }))) {
                var evt = new TestEvent();
                hub.Raise(evt);
                var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Same(evt, result);
            }
        }

        private sealed class CatchAllRelay : ICatchAllDomainEventListener {
            private readonly Func<IDomainEvent, ValueTask> _onEvent;

            public CatchAllRelay(Func<IDomainEvent, ValueTask> onEvent) {
                _onEvent = onEvent ?? throw new ArgumentNullException(nameof(onEvent));
            }

            public bool CanHandle(IDomainEvent evt) => true;

            public ValueTask OnEventTriggered(IDomainEvent evt) => _onEvent(evt);

            public ValueTask OnEventHandlingError(IDomainEvent evt, Exception x) => default;
        }

        [Fact]
        public void SubscriptionHandle_Dispose_UnsubscribesListener() {
            var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
            int counter = 0;
            var handle = hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => {
                Interlocked.Increment(ref counter);
                return default(ValueTask);
            }));
            handle.Dispose();
            hub.Raise(new TestEvent());
            Thread.Sleep(100);
            Assert.Equal(0, counter);
        }

        [Fact]
        public void Raise_DoesNotInvokeUnrelatedListener() {
            var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
            bool unrelatedCalled = false;
            using (hub.SubscribeListener(InlineLambdaListener.Create<OtherEvent>(e => {
                unrelatedCalled = true;
                return default(ValueTask);
            })))
            using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => default(ValueTask)))) {
                hub.Raise(new TestEvent());
                Thread.Sleep(100);
                Assert.False(unrelatedCalled);
            }
        }
    }
}
