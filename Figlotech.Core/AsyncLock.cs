using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public class FiAsyncLock
    {
        SemaphoreSlim _semaphore = new SemaphoreSlim(1,1);
        public async Task<T> Lock<T>(Func<Task<T>> Try, Func<Exception, Task<T>> Catch = null, Func<bool, Task> Finally = null) {
            bool isSuccess = true;
            try {
                await _semaphore.WaitAsync();
                return await Try();
            } catch (Exception x) {
                isSuccess = false;
                if (Catch != null) {
                    return await Catch.Invoke(x);
                } else {
                    throw x;
                }
            } finally {
                try {
                    if (Finally != null) {
                        await Finally.Invoke(isSuccess);
                    }
                } catch (Exception fiex) {
                    Fi.Tech.Throw(fiex);
                }
                _semaphore.Release();
            }
        }

        public Task Lock(Func<Task> Try, Func<Exception, Task> Catch = null, Func<bool, Task> Finally = null) {
            return Lock<int>(async () => {
                await Try();
                return 0;
            }, async (x) => {
                await Catch(x);
                return 0;
            }, Finally);
        }
    }
}
