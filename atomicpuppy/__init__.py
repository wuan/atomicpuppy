from .atomicpuppy import (
    StreamReader,
    StreamConfigReader,
    EventRaiser,
    Event,
    EventPublisher,
    EventStoreJsonEncoder,
    SubscriptionInfoStore
)
from .errors import *

import asyncio


class AtomicPuppy:
    running = False

    def __init__(self, cfg_file, callback, loop=None):
        self.config = StreamConfigReader().read(cfg_file)

        self.callback = callback
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(maxsize=20, loop=self._loop)
        self.stream_readers = None

    def start(self, run_once=False):
        self.counter = self.config.counter_factory()
        self._event_raiser = EventRaiser(
            self._queue,
            self.counter,
            self.callback,
            self._loop
        )
        subscription_info_store = SubscriptionInfoStore(self.config, self.counter)
        self.stream_readers = [
            StreamReader(
                queue=self._queue,
                stream_name=stream_name,
                loop=self._loop,
                instance_name=self.config.instance_name,
                subscriptions_store=subscription_info_store,
                timeout=self.config.timeout
            )
            for stream_name in self.config.streams
        ]
        self.tasks = [reader.start_consuming(run_once=run_once) for reader in self.stream_readers]
        if run_once:
            self.tasks.append(self._event_raiser.consume_events())
        else:
            self.tasks.append(self._event_raiser.start())
        return asyncio.gather(*self.tasks, loop=self._loop)

    def stop(self):
        for reader in self.stream_readers:
            reader.stop()
        self._event_raiser.stop()
