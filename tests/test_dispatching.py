import asyncio
from atomicpuppy import EventRaiser, RejectedMessageException
from atomicpuppy.atomicpuppy import Event
from .fakehttp import SpyLog
from concurrent.futures import TimeoutError
from uuid import uuid4


class When_an_event_is_processed:

    the_message = None
    event_recorder = {}
    sequence_no = 43

    def given_an_event_raiser(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        self.message_id = uuid4()

        self.queue = asyncio.Queue(loop=self._loop)
        self.message_processor = EventRaiser(self.queue,
                                                  self.event_recorder,
                                                  lambda e: self.process_message(e),
                                                  self._loop)

    def because_we_add_a_message(self):
        msg = Event(self.message_id, "type", {}, "stream", self.sequence_no)
        asyncio.async(self.send_message(msg), loop=self._loop)
        self._loop.run_until_complete(self.message_processor.start())

    def it_should_have_sent_the_message(self):
        assert(self.the_message.id == self.message_id)

    def it_should_have_recorded_the_event(self):
        assert(self.event_recorder["stream"] == self.sequence_no)

    @asyncio.coroutine
    def send_message(self, e):
        yield from self.queue.put(e)

    def process_message(self, e):
        self.the_message = e
        self.message_processor.stop()


class When_an_event_is_processed_by_running_once:

    the_message = None
    event_recorder = {}
    sequence_no = 43

    def given_an_event_raiser(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        self.message_id = uuid4()

        self.queue = asyncio.Queue(loop=self._loop)
        self.message_processor = EventRaiser(self.queue,
                                                  self.event_recorder,
                                                  lambda e: self.process_message(e),
                                                  self._loop)

    def because_we_add_a_message(self):
        msg = Event(self.message_id, "type", {}, "stream", self.sequence_no)
        asyncio.async(self.send_message(msg), loop=self._loop)
        self._loop.run_until_complete(self.message_processor.consume_events())

    def it_should_have_sent_the_message(self):
        assert(self.the_message.id == self.message_id)

    def it_should_have_recorded_the_event(self):
        assert(self.event_recorder["stream"] == self.sequence_no)

    @asyncio.coroutine
    def send_message(self, e):
        yield from self.queue.put(e)

    def process_message(self, e):
        self.the_message = e



class When_a_message_is_rejected:

    event_recorder = {}

    def given_an_event_raiser(self):
        self._log = SpyLog()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.message_id = uuid4()
        self.queue = asyncio.Queue(loop=self._loop)

        self.event_raiser = EventRaiser(
            self.queue,
            self.event_recorder,
            lambda e: self.process_message(e),
            self._loop
        )

    def because_we_process_a_message(self):
        with(self._log.capture()):
            msg = Event(self.message_id, "message-type", {}, "stream", 2)
            asyncio.async(self.send_message(msg), loop=self._loop)
            self._loop.run_until_complete(self.event_raiser.start())

    def it_should_log_a_warning(self):
        m = "message-type message "+str(self.message_id) \
            +" was rejected and has not been processed"
        assert(any(r.message == m for r in self._log.warnings))

    def process_message(self, e):
        self.event_raiser.stop()
        raise RejectedMessageException()

    @asyncio.coroutine
    def send_message(self, e):
        yield from self.queue.put(e)


class When_a_message_raises_an_unhandled_exception:

    event_recorder = {}

    def given_an_event_raiser(self):
        self._log = SpyLog()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.message_id = uuid4()
        self.queue = asyncio.Queue(loop=self._loop)

        self.event_raiser = EventRaiser(
            self.queue,
            self.event_recorder,
            lambda e: self.process_message(e),
            self._loop
        )

    def because_we_process_a_message(self):
        with(self._log.capture()):
            msg = Event(self.message_id, "message-type", {}, "stream", 2)
            asyncio.async(self.send_message(msg), loop=self._loop)
            self._loop.run_until_complete(self.event_raiser.start())

    def it_should_log_an_error(self):
        m = "Failed to process message "
        assert(any(r.message.startswith(m) for r in self._log.errors))

    def process_message(self, e):
        self.event_raiser.stop()
        raise NotImplemented("This handler is not here")

    @asyncio.coroutine
    def send_message(self, e):
        yield from self.queue.put(e)


