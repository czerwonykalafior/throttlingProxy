import asyncio

import aioredis

from redis_client import active_domains


async def main():
    redis_conn = aioredis.from_url("redis://localhost:6379")
    domain_status = {}
    async with redis_conn.client() as r:
        while True:
            for domain in active_domains:
                domain_status[domain.name] = await r.get(
                    f"{domain.name}_queue"
                ), await r.llen(domain.name)

            print(domain_status)

            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
