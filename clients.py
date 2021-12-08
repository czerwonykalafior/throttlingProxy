import asyncio
from typing import List

from proxy import Request, proxy

requests = [
    *[Request("amazon", f"http://amazon.com/item/{req_no}") for req_no in range(100)],
    *[Request("google", f"http://google.com/item/{req_no}") for req_no in range(100)],
]


async def make_requests(requests: List[Request]):
    tasks = [proxy(request) for request in requests]

    for task in asyncio.as_completed(tasks):
        result = await task
        print(result)


asyncio.run(make_requests(requests))
