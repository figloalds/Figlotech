using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Figlotech.Core.Tests {

    public class ParallelFlowTests {

        [Fact]
        public async Task ParallelFlow_WithEnumerableSource_AwaitsAllItems() {
            var input = Enumerable.Range(1, 10).ToList();

            var result = await Fi.Tech.ParallelFlow<int>(input)
                .Then(x => x * 2);

            Assert.Equal(10, result.Count);
            Assert.Equal(input.Select(x => x * 2).OrderBy(x => x), result.OrderBy(x => x));
        }

        [Fact]
        public async Task ParallelFlow_WithGeneratorSource_AwaitsAllItems() {
            var result = await Fi.Tech.ParallelFlow<int>((yield) => {
                yield.ReturnRange(Enumerable.Range(1, 10));
            }).Then(x => x * 2);

            Assert.Equal(10, result.Count);
            Assert.Equal(Enumerable.Range(1, 10).Select(x => x * 2).OrderBy(x => x), result.OrderBy(x => x));
        }

        [Fact]
        public async Task ParallelFlow_WithAsyncGeneratorSource_AwaitsAllItems() {
            var result = await Fi.Tech.ParallelFlow<int>(async (yield) => {
                await Task.Yield();
                yield.ReturnRange(Enumerable.Range(1, 10));
            }).Then(x => x * 2);

            Assert.Equal(10, result.Count);
        }

        [Fact]
        public async Task ParallelFlow_WithMultipleSteps_AwaitsFinalResults() {
            var input = Enumerable.Range(1, 5).ToList();

            var result = await Fi.Tech.ParallelFlow<int>(input)
                .Then(x => x + 1)
                .Then(async x => {
                    await Task.Yield();
                    return x * 2;
                })
                .Then(x => x.ToString());

            Assert.Equal(5, result.Count);
            Assert.Equal(input.Select(x => ((x + 1) * 2).ToString()).OrderBy(x => x), result.OrderBy(x => x));
        }

        [Fact]
        public async Task ParallelFlow_AsyncEnumerable_YieldsAllItems() {
            var input = Enumerable.Range(1, 5).ToList();
            var collected = new List<int>();

            await foreach (var item in Fi.Tech.ParallelFlow<int>(input).Then(x => x * 2)) {
                collected.Add(item);
            }

            Assert.Equal(5, collected.Count);
            Assert.Equal(input.Select(x => x * 2).OrderBy(x => x), collected.OrderBy(x => x));
        }

        [Fact]
        public async Task ParallelFlow_EmptySource_Completes() {
            var result = await Fi.Tech.ParallelFlow<int>(Array.Empty<int>())
                .Then(x => x * 2);

            Assert.Empty(result);
        }
    }
}
