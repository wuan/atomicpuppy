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
        """
        :param cfg_file Config file must be either a string containing the path to the config file or a
                        dictionary containing a "atomicpuppy" index.

        :param callback The callback that will be called for each event. This function will be called
                        with a atomicpuppy.Event object as a parameter.

        :param loop     asyncio event loop
        """
        self.config = StreamConfigReader().read(cfg_file)

        self.callback = callback
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(maxsize=20, loop=self._loop)
        self.stream_readers = None

    def start(self, run_once=False):
        """
        Entrypoint to atomicpuppy package. This will create all the event readers based on provided config
        and will prepare the tasks for asyncio.

        :param run_once Set this to True to consume the stream and return, rather than having an ongoing
                        process of consumption.

        To be used with asyncio loop:
        >>> loop = asyncio.get_event_loop()
        >>> atomic_puppy = AtomicPuppy('config.yaml', callback)
        >>> loop.run_until_complete(atomic_puppy.start())
        """

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
