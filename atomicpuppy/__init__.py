from .atomicpuppy import (
    Event,
    EventCounter,
    EventPublisher,
    EventRaiser,
    EventStoreJsonEncoder,
    RedisCounter,
    StreamConfigReader,
    StreamFetcher,
    StreamReader,
    SubscriptionInfoStore,
    EventFinder as EventFinder_,
)
from .errors import (
    FatalError,
    HttpClientError,
    HttpNotFoundError,
    HttpServerError,
    RejectedMessageException,
    StreamNotFoundError,
    UrlError,
)

import asyncio


class EventFinder:

    def __init__(self, cfg_file, loop=None):
        """
        cfg_file: dictionary or filename or yaml text
        """
        self._config = StreamConfigReader().read(cfg_file)
        self._loop = loop or asyncio.get_event_loop()

    @asyncio.coroutine
    def find_backwards(self, stream, predicate, predicate_label='predicate'):
        instance_name = (
            self._config.instance_name + ' find_backwards {}'.format(stream)
        )
        fetcher = StreamFetcher(
            None, loop=self._loop, nosleep=False, timeout=self._config.timeout)
        head_uri = 'http://{}:{}/streams/{}/head/backward/{}'.format(
            self._config.host,
            self._config.port,
            stream,
            self._config.page_size)
        finder = EventFinder_(
            fetcher=fetcher,
            stream_name=stream,
            loop=self._loop,
            instance_name=instance_name,
            head_uri=head_uri,
        )
        return (yield from
                finder.find_backwards(stream, predicate, predicate_label))


class AtomicPuppy:

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


__all__ = [
    AtomicPuppy,
    Event,
    EventCounter,
    EventFinder,
    EventPublisher,
    EventStoreJsonEncoder,
    FatalError,
    HttpClientError,
    HttpNotFoundError,
    HttpServerError,
    RedisCounter,
    RejectedMessageException,
    StreamNotFoundError,
    UrlError,
]
