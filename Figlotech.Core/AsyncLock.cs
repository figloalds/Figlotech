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
        public async Task Lock(Func<Task> Try, Func<Exception, Task> Catch = null, Func<bool, Task> Finally = null) {
            bool isSuccess = false;
            try {
                await _semaphore.WaitAsync();
                await Try();
                isSuccess = true;
            } catch (Exception x) {
                if (Catch != null) {
                    await Catch.Invoke(x);
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
    }
}
