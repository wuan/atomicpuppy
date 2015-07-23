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
        with open(cfg_file) as file:
            self.config = StreamConfigReader().read(file)
        self.callback = callback
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(maxsize=20, loop=self._loop)

    def start(self):
        c = self.counter = self.config.counter_factory()
        self._messageProcessor = EventRaiser(self._queue,
                                             c,
                                             self.callback,
                                             self._loop)
        subscription_info_store = SubscriptionInfoStore(self.config, c)
        self.readers = [
            StreamReader(
                queue=self._queue,
                stream_name=s,
                loop=self._loop,
                instance_name=self.config.instance_name,
                subscriptions_store=subscription_info_store,
                timeout=self.config.timeout)
            for s in self.config.streams
        ]
        self.tasks = [s.start_consuming() for s in self.readers]
        self.tasks.append(self._messageProcessor.start())
        return asyncio.gather(*self.tasks, loop=self._loop)

    def stop(self):
        for s in self.readers:
            s.stop()
        self._messageProcessor.stop()
