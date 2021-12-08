import asyncio
from collections import namedtuple

import aioredis

ActiveDomain = namedtuple("RegisterDomain", ["name", "concurrent_requests"])
active_domains = [ActiveDomain("amazon", 5), ActiveDomain("google", 10)]


async def main():
    await hard_reset()
    redis_connection = aioredis.from_url("redis://localhost:6379")
    while True:
        await asyncio.sleep(
            2
        )  # this sleep values is actual throttling for given domain, priority queue can solve this
        await add_quota(redis_connection)


async def add_quota(redis_connection):
    async with redis_connection.pipeline() as pipeline:
        # TODO: for now semi High Availability can be done as - separate instance process/throttle some part for domain_status

        # [pipeline.delete(domain.name) for domain in active_domains]
        # same as trim but probably quicker but next lpush has to create list every time...

        [
            pipeline.lpush(domain.name, 1)
            for domain in active_domains
            for _ in range(domain.concurrent_requests)
        ]

        [
            pipeline.ltrim(domain.name, 0, domain.concurrent_requests)
            for domain in active_domains
        ]
        # in case quota was not consumed we don't want to add to infinity

        result = await pipeline.execute()

        print("Quotas refilled.", result)


async def hard_reset():
    redis_connection = aioredis.from_url("redis://localhost:6379")
    async with redis_connection.client() as r:
        for domain in active_domains:

            reset_queue = await r.set(f"{domain.name}_queue", 0)
            reset_quota = await r.delete(domain.name)
            print(reset_queue, reset_quota)


if __name__ == "__main__":
    asyncio.run(main())
