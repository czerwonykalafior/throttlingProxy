# const ;
# // accuire semaphore for you domain, if queue is empty, wait (non blocking)
# //
# // table with:
# // DOMAIN |  rate limit  |  max_per_month
# // throttlingManager:
# // On new/changne domain (throttlingManager.onDomainChange()):
# // - query db every X sek (this time step should match max timeout on robot requets otherwise it will timeout
# cus there will be no semaphore for this doman)
# // - call trotthlingManager on domain model change
# //
# // throttlingManager.registerDomain(domain_name, strategy)
# registered_domains = {"domain_name": strategy}
#
# // throttlingManager.replenish()
# // iterate over priorityQueue and
# // replenish domains that their time came (next_replenish_at <= now()) AND add next next_replenish_at for
# this domain to the priorityQueue
# //
import time
from functools import total_ordering
from queue import PriorityQueue
from typing import List

import aioredis

available_quotas = {
    "amazon.com": [1, 1, 1, 1, 1],
    "google.com": [1, 1, 1],
    "puccinos": [1],
}


@total_ordering
class RequestQuota:
    def __init__(self, domain: str, next_request_at: float, concurrent_requests: int):
        self.domain = domain
        self.next_req = next_request_at
        self.concurrent_req = concurrent_requests

    def __eq__(self, other):
        return self.next_req == other.next_req

    def __lt__(self, other):
        return self.next_req < other.next_req


class ThrottlingManager:
    def __init__(self):
        self.registered_domains = {}  # TODO: get all domains for DB
        self.next_quota_reset: PriorityQueue[RequestQuota] = PriorityQueue()
        self.redis_connection = aioredis.from_url("redis://localhost:6379")

    def __on_domain_change(self) -> None:
        """ Subscribe on domain throttling table. """
        domains = {}  # new or changed
        for domain, strategy in domains:
            await self._remove_quota(domain)
            self._register_domain(domain, strategy)

    def _register_domain(self, domain: str, strategy: str) -> None:
        self.registered_domains[domain] = strategy
        self._schedule_next_quota(domain=domain, previous_req=time.time() - 1000)

    def _schedule_next_quota(self, domain: str, previous_req: float) -> None:
        throttling_strategy = self.registered_domains[domain]
        next_request_time, concurrent_requests_count = throttling_strategy(previous_req)
        self.next_quota_reset.put(RequestQuota(domain, next_request_time, concurrent_requests_count))

    def refill_quota(self) -> None:
        while self.next_quota_reset:
            quota = self.next_quota_reset.queue[0]

            if quota.next_req <= time.time():
                quota = self.next_quota_reset.get()  # optimization: take from the queue if it's the right time
                self.add_quota(quota)  # TODO: optimization: get all quotas that pass the time and add in batch
                self._schedule_next_quota(domain=quota.domain, previous_req=quota.next_req)
                # TODO: optimization: this we can add new items once we're done replenishing

            # TODO: every 1 min check for empty `domain`_queue if queue is empty X times in a row unregister this domain

    async def add_quota(self, quota: RequestQuota):
        async with self.redis_connection.pipeline() as pipeline:
            # TODO: for now semi High Availability can be done as - separate instance process/throttle some part for domain_status

            [pipeline.lpush(quota.domain, 1) for _ in range(quota.concurrent_req)]
            pipeline.ltrim(quota.domain, 0, quota.concurrent_req)
            quota_added = await pipeline.execute()

            print(f"Quotas for {quota.domain} refilled.", quota_added)

    async def _remove_quota(self, domain: str) -> None:
        async with self.redis_connection.client() as r:
            await r.delete(domain)
            


# quotas only for active robots? when starting a robot call Throttling manager?
# how to make this work for KAPOW or anything
# proxy could create and empty list but than manager will have to ping redis every tick for active
# proxy can call manager to register new domain when there is no redis list to hang on. It will slow down first request.
