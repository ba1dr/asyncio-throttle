
import time
import asyncio
from collections import deque
from itertools import cycle


class Throttler(object):

    def __init__(self, rate_limit, period=1.0, name=None):
        self.rate_limit = rate_limit
        self.period = period
        self.name = name

        self._task_logs = deque()

    def flush(self):
        now = time.time()
        while self._task_logs:
            if now - self._task_logs[0] > self.period:
                self._task_logs.popleft()
            else:
                break

    def is_ready(self):
        self.flush()
        return len(self._task_logs) < self.rate_limit

    async def acquire(self, wait=True):
        while True:
            if self.is_ready():
                break

            if not wait:
                return False

            sleeptime = self.period - (time.time() - self._task_logs[0])
            if sleeptime > 0:
                await asyncio.sleep(sleeptime)

        self._task_logs.append(time.time())
        return True

    async def __aenter__(self):
        await self.acquire(wait=True)

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(0)


class ThrottlerPool(object):
    """
        Might be useful to serve multiple homogeniuos
            resources (e.g server pool) with same rate limit
    """

    def __init__(self, rate_limit, period=1.0,
                 names=None, retry_interval=0.01):
        self._pool = []
        self.rate_limit = rate_limit
        self.period = period
        self.retry_interval = retry_interval
        names = names or []
        for name in names:
            self._pool.append(Throttler(rate_limit=rate_limit,
                                        period=period,
                                        name=name))

    def cyclepool(self):
        # balancing the load
        for thr in cycle(self._pool):
            yield thr

    async def acquire(self):
        while True:
            for thr in self.cyclepool():
                if await thr.acquire(wait=False):
                    return thr.name

            await asyncio.sleep(self.retry_interval)

    async def __aenter__(self):
        return await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(0)
