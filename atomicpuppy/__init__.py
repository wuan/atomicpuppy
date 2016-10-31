from .atomicpuppy import (
    StreamReader,
    StreamConfigReader,
    EventRaiser,
    Event,
    EventPublisher,
    EventStoreJsonEncoder,
    SubscriptionInfoStore,
    RedisCounter
)
from .errors import *

import asyncio


class EventFinder:

    def __init__(self, cfg_file, loop=None):
        """
        cfg_file: dictionary or filename or yaml text
        """
        self._config = StreamConfigReader().read(cfg_file)
        self._loop = loop or asyncio.get_event_loop()

    @asyncio.coroutine
    def find_forwards(self, stream, predicate, predicate_label='predicate'):
        # This import is here to avoid polluting the package namespace
        from .atomicpuppy import InMemoryAutoIncrementingSingleStreamCounter
        instance_name = (
            self._config.instance_name + ' find_forwards {}'.format(stream)
        )
        queue = asyncio.Queue(maxsize=20, loop=self._loop)
        c = InMemoryAutoIncrementingSingleStreamCounter(stream)
        subscription_info_store = SubscriptionInfoStore(self._config, c)
        reader = StreamReader(
            queue=queue,
            stream_name=stream,
            loop=self._loop,
            instance_name=instance_name,
            subscriptions_store=subscription_info_store,
            timeout=self._config.timeout
        )
        subscription = subscription_info_store.get(stream)
        return (yield from reader.find_forwards(
            subscription.uri, predicate, predicate_label))


class AtomicPuppy:

    running = False

    def __init__(self, cfg_file, callback, loop=None):
        """
        cfg_file: dictionary or filename or yaml text
        """
        self.config = StreamConfigReader().read(cfg_file)
        self.callback = callback
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(maxsize=20, loop=self._loop)

    def start(self, run_once=False):
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
        self.tasks = [s.start_consuming(once=run_once) for s in self.readers]
        if run_once:
            self.tasks.append(self._messageProcessor.consume_events())
        else:
            self.tasks.append(self._messageProcessor.start())
        return asyncio.gather(*self.tasks, loop=self._loop)

    def stop(self):
        for s in self.readers:
            s.stop()
        self._messageProcessor.stop()
