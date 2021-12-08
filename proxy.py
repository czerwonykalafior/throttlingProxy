import time
from collections import namedtuple

import aioredis

redis_pool = aioredis.from_url("redis://localhost:6379")

Request = namedtuple("Request", ["domain", "url"])


async def aquire_quota(domain: str):
    async with redis_pool.pipeline(transaction=False) as r:
        r.incr(f"{domain}_queue", 1)
        r.brpop(domain)
        r.decr(f"{domain}_queue", 1)
        await r.execute()


async def proxy(request: Request):
    start = time.time()
    print(f"Processing request {request} at", start)
    await aquire_quota(request.domain)
    end = time.time()
    print(f"Time to process request {request} {end-start:.2f}")
    return f"{request} processed"
