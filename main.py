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
        self.next_request_at = next_request_at
        self.concurrent_requests = concurrent_requests

    def __eq__(self, other):
        return self.next_request_at == other.next_request_at

    def __lt__(self, other):
        return self.next_request_at < other.next_request_at


class ThrottlingManager:
    def __init__(self):
        self.registered_domains = {}  # TODO: this needs work
        self.next_quota_reset: PriorityQueue[RequestQuota] = PriorityQueue()
        self.registered_domains = aioredis.from_url("redis://localhost:6379")

    def on_domain_change(self, domain: str, strategy: str) -> None:
        self._register_domain(domain, strategy)
        self._remove_quota(domain)

    def _register_domain(self, domain: str, strategy: str) -> None:
        self.registered_domains[domain] = strategy
        self._schedule_next_reset(
            domain=domain, previous_request_at=time.time() - 1000
        )  # whatever in the past

    def _schedule_next_reset(self, domain: str, previous_request_at: float) -> None:
        throttling_strategy = self.registered_domains[domain]
        next_request_time, concurrent_requests_count = throttling_strategy(
            previous_request_at
        )
        self.next_quota_reset.put(
            RequestQuota(domain, next_request_time, concurrent_requests_count)
        )

    def refill_quota(self) -> None:
        to_be_refiled = []

        while self.next_quota_reset:
            quota = self.next_quota_reset.get()
            if quota.next_request_at <= time.time():
                to_be_refiled.append(quota)

                # to optimize this we can add new items once we're done replenishing
            else:
                self.next_quota_reset.put(quota)  # put it back
                self.add_quota(to_be_refiled)

                for quota in to_be_refiled:
                    self._schedule_next_reset(
                        domain=quota.domain, previous_request_at=quota.next_request_at
                    )
                to_be_refiled = []

            # TODO: every 1 min check for empty `domain`_queue if queue is empty X times in a row unregister this domain

    async def add_quota(self, quotas: List[RequestQuota]):
        async with self.redis_connection.pipeline() as pipeline:
            # TODO: for now semi High Availability can be done as - separate instance process/throttle some part for domain_status

            [
                pipeline.lpush(quota.domain, 1)
                for quota in quotas
                for _ in range(quota.concurrent_requests)
            ]

            [
                pipeline.ltrim(quota.domain, 0, quota.concurrent_requests)
                for quota in quotas
            ]
            # in case quota was not consumed we don't want to add to infinity

            result = await pipeline.execute()

            print("Quotas refilled.", result)

    @staticmethod
    def _remove_quota(domain: str) -> None:
        """Remove a list from available_semaphores"""
        del available_quotas[domain]


# quotas only for active robots? when starting a robot call Throttling manager?
# how to make this work for KAPOW or anything
# proxy could create and empty list but than manager will have to ping redis every tick for active
# proxy can call manager to register new domain when there is no redis list to hang on. It will slow down first request.
