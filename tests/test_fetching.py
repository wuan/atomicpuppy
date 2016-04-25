import aiohttp
import asyncio
import datetime
import logging
import os
from unittest.mock import patch
from uuid import UUID, uuid4

import fakeredis
from freezegun import freeze_time

from atomicpuppy.atomicpuppy import (
    StreamReader, SubscriptionInfoStore, SubscriptionConfig, RedisCounter
)
from .fakehttp import FakeHttp, SpyLog

SCRIPT_PATH = os.path.dirname(__file__)


class StreamReaderContext:

    _loop = None
    _events = None
    _host = 'eventstore.local'
    _port = 2113

    def __init__(self):
        self.counter = RedisCounter(fakeredis.FakeStrictRedis(), "test-instace-{}".format(uuid4()))

    def given_an_event_loop(self):
        logging.basicConfig(filename='example.log', level=logging.DEBUG)
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        self.http = FakeHttp(self._loop)
        self._queue = asyncio.Queue(loop=self._loop)

    def subscribe_and_run(self, stream, last_read=-1, nosleep=False):
        self.subscribeTo(stream, last_read, nosleep)
        self.run_the_reader()

    def run_the_reader(self):
        mock = self.http.getMock()
        with patch("aiohttp.request", new=mock):
            self._loop.run_until_complete(
                self._reader.start_consuming(once=True)
            )

    def create_counter(self):
        return lambda: self.counter

    def subscribeTo(self, stream, last_read, nosleep=False):
        assert(last_read is not None)
        config = SubscriptionConfig(
            streams=None,
            counter_factory=self.create_counter,
            instance_name='foo',
            host=self._host,
            port=self._port,
            timeout=20)

        subscriptions_store = SubscriptionInfoStore(config, self.counter)
        if last_read != -1:
            self.counter[stream] = last_read
        self._reader = StreamReader(
            queue=self._queue,
            stream_name=stream,
            loop=self._loop,
            instance_name='foo',
            subscriptions_store=subscriptions_store,
            timeout=config.timeout,
            nosleep=nosleep)
        return self._reader


class When_a_stream_contains_a_single_event_and_the_counter_is_at_the_start(StreamReaderContext):

    _event = None

    @property
    def the_event(self):
        if(not self._event):
            self._event = self._queue.get_nowait()
        return self._event

    def given_a_feed_containing_one_event(self):
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            SCRIPT_PATH + '/responses/single-event.json')

    def because_we_start_the_reader(self):
        self.subscribe_and_run('newstream')

    def it_should_contain_the_body(self):
        assert(self.the_event.data == {"a": 1})

    def it_should_contain_the_event_type(self):
        assert(self.the_event.type == "my-event")

    def it_should_contain_the_event_id(self):
        assert(
            self.the_event.id == UUID('fbf4a1a1-b4a3-4dfe-a01f-ec52c34e16e4'))

    def it_should_contain_the_sequence_number(self):
        assert(self.the_event.sequence == 0)

    def it_should_contain_the_stream_id(self):
        assert(self.the_event.stream == "newstream")


class When_an_event_contains_no_data(StreamReaderContext):

    _event = None

    def given_a_feed_containing_one_event(self):
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            SCRIPT_PATH + '/responses/single-event-no-data.json')

    def because_we_start_the_reader(self):
        self._log = SpyLog()
        with(self._log.capture()):
            self.subscribe_and_run('newstream')

    def it_should_return_a_none_event(self):
        assert(self._queue.empty())

    def it_should_log_a_warning(self):
        for r in self._log._logs:
            print(r.msg)
        assert(any(r.msg.startswith("No `data` key found on event")
                   and r.levelno == logging.WARNING
                   for r in self._log._logs))

class When_a_feed_contains_multiple_events(StreamReaderContext):

    _the_events = []

    def given_a_feed_containing_three_events(self):
        self.http.registerJsonUri('http://eventstore.local:2113/streams/foo/0/forward/20',
                                  SCRIPT_PATH + '/responses/three-events.json')

    def because_we_start_the_reader(self):
        self.subscribe_and_run('foo')

    def it_should_return_three_events(self):
        assert(len(self.the_events) == 3)

    def the_events_should_be_ordered_correctly(self):
        assert(self.the_events[0].sequence == 0)
        assert(self.the_events[1].sequence == 1)
        assert(self.the_events[2].sequence == 2)

    @property
    def the_events(self):
        if(not self._the_events):
            while(not self._queue.empty()):
                self._the_events.append(self._queue.get_nowait())

        return self._the_events


class When_a_last_read_event_is_specified(StreamReaderContext):

    _the_events = []

    def given_a_feed_containing_three_events(self):
        self.http.registerJsonUri('http://eventstore.local:2113/streams/foo/1/forward/20',
                                  SCRIPT_PATH + '/responses/three-events.json')
        self.http.registerJsonUri(
            'http://127.0.0.1:2113/streams/newstream2/3/forward/20',
            SCRIPT_PATH + '/responses/two-page/head_next_prev_prev.json')

    def because_we_start_the_reader(self):
        self.subscribe_and_run('foo', last_read=1)

    def it_should_return_one_event(self):
        assert(len(self.the_events) == 1)

    def the_events_should_be_ordered_correctly(self):
        assert(self.the_events[0].sequence == 2)

    @property
    def the_events(self):
        if(not self._the_events):
            while(not self._queue.empty()):
                self._the_events.append(self._queue.get_nowait())

        return self._the_events


# For some reason (ie. because I deleted things), my stock feed got a bit messed up
# but that gives us a useful test case if nowt else
# class When_a_feed_spans_several_pages(StreamReaderContext):

#     _the_events = []

#     def given_a_feed_spanning_two_pages(self):
#         self._host = "127.0.0.1"

#         # all the pages except the first and last in this list are empty.
#         self.http.registerJsonUris(
#             {
#                 'http://127.0.0.1:2113/streams/stock':
#                     SCRIPT_PATH + '/responses/two-page/head.json',
#                 'http://127.0.0.1:2113/streams/stock/0/forward/20':
#                     SCRIPT_PATH + '/responses/two-page/head_last.json',
#                  'http://127.0.0.1:2113/streams/stock/20/forward/20':
#                     SCRIPT_PATH + '/responses/two-page/head_last_prev.json',
#                  'http://127.0.0.1:2113/streams/stock/40/forward/20':
#                     SCRIPT_PATH + '/responses/two-page/head_last_prev_prev.json',
#                  'http://127.0.0.1:2113/streams/stock/60/forward/20':
#                     SCRIPT_PATH + '/responses/two-page/head_last_prev_prev_prev.json',
#                  'http://127.0.0.1:2113/streams/stock/80/forward/20':
#                     SCRIPT_PATH + '/responses/two-page/head_last_prev_prev_prev_prev.json',
#                  'http://127.0.0.1:2113/streams/stock/84/forward/20':
#                     SCRIPT_PATH + '/responses/two-page/head_last_prev_prev_prev_prev_prev.json',
#             }
#         )

#     def because_we_start_the_reader(self):
#         self.subscribe_and_run('stock')

#     def we_should_raise_all_the_events(self):
#         assert(len(self.the_events) == 22)

#     def we_should_raise_the_events_in_the_correct_order(self):
#         assert(self.the_events[0].sequence == 62)
#         assert(self.the_events[21].sequence == 83)

#     @property
#     def the_events(self):
#         if(not self._the_events):
#             while(not self._queue.empty()):
#                 self._the_events.append(self._queue.get_nowait())

#         return self._the_events


class When_the_last_read_event_is_on_the_first_page(StreamReaderContext):

    _the_events = []

    def given_a_feed_spanning_two_pages(self):
        self._host = "127.0.0.1"

        self.http.registerJsonUris({
            'http://127.0.0.1:2113/streams/stock/80/forward/20':
            SCRIPT_PATH + '/responses/two-page/head.json',
            'http://127.0.0.1:2113/streams/stock/84/forward/20':
            SCRIPT_PATH + '/responses/two-page/head_next_prev_prev.json',


            })

    def because_we_start_the_reader(self):
        self.subscribe_and_run('stock', last_read=80)

    def we_should_raise_the_new_events(self):
        assert(len(self.the_events) == 3)

    def we_should_raise_the_events_in_the_correct_order(self):
        assert(self.the_events[0].sequence == 81)
        assert(self.the_events[2].sequence == 83)

    @property
    def the_events(self):
        if(not self._the_events):
            while(not self._queue.empty()):
                self._the_events.append(self._queue.get_nowait())

        return self._the_events


class When_the_reader_is_invoked_for_a_second_time(StreamReaderContext):

    _event = None

    @property
    def the_event(self):
        if(not self._event):
            self._event = self._queue.get_nowait()
        return self._event

    def given_a_reader_that_has_read_all_the_events(self):
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            SCRIPT_PATH + '/responses/single-event.json')
        self.http.registerJsonUri(
            'http://127.0.0.1:2113/streams/newstream/1/forward/20',
            SCRIPT_PATH + '/responses/empty.json')
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            SCRIPT_PATH + '/responses/single-event.json')
        self.http.registerJsonUri(
            'http://127.0.0.1:2113/streams/newstream/1/forward/20',
            SCRIPT_PATH + '/responses/empty.json')

        self._reader = self.subscribeTo('newstream', -1)
        self.run_the_reader()
        self._queue.get_nowait()

    def because_we_run_the_reader_a_second_time(self):
        self.run_the_reader()

    def it_should_not_return_any_events(self):
        assert(self._queue.empty())

    def run_the_reader(self):
        mock = self.http.getMock()
        with patch("aiohttp.request", new=mock):
            self._loop.run_until_complete(
                self._reader.start_consuming(once=True)
            )


"""
In this context, we create a new stream containing a single event (head.json)
When we poll the stream a second time there are no new events, so we walk to the
previous link(head_prev.json, then head_prev2.json)

When we poll the stream a third time, there are 23 new events. Since that's more
than a page worth, we should first seek the last_read event on the next page
(head_prev2_next) then walk backward until we find a new empty page
(head_prev2_next_prev_prev.json)

"""


class When_events_are_added_after_the_first_run(StreamReaderContext):

    _the_events = []

    def given_a_changing_feed(self):
        # When we first hit the stream, it returns a single event
        self.http.registerJsonUri('http://eventstore.local:2113/streams/stock/0/forward/20',
                                  SCRIPT_PATH + '/responses/new-events/head.json')

        self.http.registerJsonUri('http://eventstore.local:2113/streams/stock/0/forward/20',
                                  SCRIPT_PATH + '/responses/new-events/head.json')

        self.http.registerJsonUri('http://eventstore.local:2113/streams/stock/0/forward/20',
                                  SCRIPT_PATH + '/responses/new-events/head.json')

        # On the second invocation, we receive no new events
        # On the third invocation, we receive two pages of two events each
        self.http.registerJsonsUri(
            'http://127.0.0.1:2113/streams/stock/85/forward/20',
            [SCRIPT_PATH + '/responses/new-events/head_prev.json',
             SCRIPT_PATH + '/responses/new-events/head_prev2.json',
             SCRIPT_PATH + '/responses/new-events/head_prev2.json', ])
        self.http.registerJsonUri(
            'http://127.0.0.1:2113/streams/stock/84/backward/20',
            SCRIPT_PATH + '/responses/new-events/head_prev2_next.json')
        self.http.registerJsonUri(
            'http://127.0.0.1:2113/streams/stock/105/forward/20',
            SCRIPT_PATH + '/responses/new-events/head_prev2_next_prev.json')
        self.http.registerJsonUri(
            'http://127.0.0.1:2113/streams/stock/108/forward/20',
            SCRIPT_PATH + '/responses/new-events/head_prev2_next_prev_prev.json')

    def because_we_run_the_reader_three_times(self):
        self._reader = self.subscribeTo('stock', -1)

        self.run_the_reader()
        self.run_the_reader()
        self.run_the_reader()

    def it_should_have_read_all_the_events(self):
        assert(len(self.the_events) == 24)

    def it_should_have_read_the_events_in_the_correct_order(self):
        assert(self.the_events[0].sequence == 84)
        assert(self.the_events[23].sequence == 107)

    def run_the_reader(self):
        mock = self.http.getMock()
        with patch("aiohttp.request", new=mock):
            self._loop.run_until_complete(
                self._reader.start_consuming(once=True)
            )

    @property
    def the_events(self):
        if(not self._the_events):
            while(not self._queue.empty()):
                self._the_events.append(self._queue.get_nowait())

        return self._the_events



class When_reading_from_a_category_projection(StreamReaderContext):

    _the_events = []

    @property
    def the_events(self):
        if(not self._the_events):
            while(not self._queue.empty()):
                self._the_events.append(self._queue.get_nowait())

        return self._the_events

    def given_a_category_projection_stream(self):
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/$ce-order/0/forward/20',
            SCRIPT_PATH + '/responses/category-projection/head.json'
        )

    def because_we_run_the_reader(self):
        self._reader = self.subscribeTo('$ce-order', -1)
        self.run_the_reader()

    def it_should_have_read_all_the_events(self):
        assert len(self.the_events) == 3

    def it_should_have_read_the_events_in_the_correct_order(self):
        for seq in range(3):
            assert self.the_events[seq].sequence == seq

    def it_should_use_the_category_projection_stream_name(self):
        for seq in range(3):
            assert self.the_events[seq].stream == '$ce-order'


"""
Value errors pretty much mean that our URL is screwed, or that there's an SSL
context mismatch. In that case, we should just end the loop.
"""


class When_a_valueerror_occurs_during_fetch(StreamReaderContext):

    _log = SpyLog()

    def given_a_malformed_port(self):
        self._port = "tawny"

    # note that we run with a real event loop, and don't explicitly
    # call stop. The exception will stop the loop.
    def because_we_start_the_reader(self):
        self._reader = self.subscribeTo("my-stream", 1)
        with(self._log.capture()):
            self._loop.run_until_complete(
                self._reader.start_consuming()
                )

    def it_should_log_a_critical_error(self):
        assert(filter(lambda r: r.level == logging.CRITICAL, self._log._logs))


"""
If we get a client error, then something has gone wrong with the http
layer processing. It's almost certainly an intermittent fault and we should
retry.
"""


class When_a_client_error_occurs_during_fetch(StreamReaderContext):

    _log = SpyLog()

    def given_a_client_error(self):
        self.http.registerCallbacksUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            [
                lambda: exec('raise aiohttp.errors.ClientOSError("Darn it, can\'t connect")'),
                lambda: exec('raise ValueError()')
            ]
        )

    def because_we_start_the_reader(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_warning(self):
        for r in self._log._logs:
            print(r.msg)
        assert(any(r.msg == "Error occurred while requesting %s"
                   and r.levelno == logging.WARNING
                   for r in self._log._logs))


class When_multiple_errors_of_the_same_type_occur(StreamReaderContext):

    _log = SpyLog()

    def given_a_client_error(self):
        self.http.registerCallbacksUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            [
                lambda: exec('raise aiohttp.errors.ClientOSError("Darn it, can\'t connect")'),
                lambda: exec('raise aiohttp.errors.ClientOSError("Darn it, can\'t connect")'),
                lambda: exec('raise ValueError()')
            ]
        )

    def because_we_start_the_reader(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_warning(self):
        for r in self._log._logs:
            print(r.msg)
        assert(len([r for r in self._log._logs if r.msg.startswith("Error occurred while requesting %s")]) == 1)


"""
If we get a disconnection error, then it's a network level issue. Retry with
a backoff.
"""


class When_a_disconnection_error_occurs_during_fetch(StreamReaderContext):

    _log = SpyLog()

    def given_a_disconnection_error(self):
        self.http.registerCallbacksUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            [
                lambda: exec('raise aiohttp.errors.DisconnectedError("Darn it, can\'t connect")'),
                lambda: exec('raise ValueError()')
            ]
        )

    def because_we_start_the_reader(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_warning(self):
        assert(any(r.msg == "Error occurred while requesting %s"
                   and r.levelno == logging.WARNING
                   for r in self._log._logs))


class When_a_timeout_error_occurs_during_fetch(StreamReaderContext):

    """
    If we get a Timeout error, then it's a network level issue. Retry with
    a backoff.
    """

    _log = SpyLog()

    def given_a_timeout_error(self):
        self.http.registerCallbacksUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            [
                lambda: exec('raise aiohttp.errors.TimeoutError()'),
                lambda: exec('raise ValueError()')
            ]
        )

    def because_we_start_the_reader(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_warning(self):
        assert(any(r.msg == "Error occurred while requesting %s"
                   and r.levelno == logging.WARNING
                   for r in self._log._logs))


"""
If we get a ClientResponseError error, then it's a network level issue. Retry with
a backoff.
"""


class When_a_client_response_error_occurs_during_fetch(StreamReaderContext):

    _log = SpyLog()

    def given_a_client_response_error(self):
        self.http.registerCallbacksUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            [
                lambda: exec('raise aiohttp.errors.ClientResponseError("Darn it, something went bad")'),
                lambda: exec('raise ValueError()')
            ]
        )

    def because_we_start_the_reader(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_warning(self):
        assert(any(r.msg == "Error occurred while requesting %s"
                   and r.levelno == logging.WARNING
                   for r in self._log._logs))


"""
If we get a 40x range message then something is wrong with our configuration,
we should stop the loop and log an error.
"""


class When_we_receive_a_4xx_range_error(StreamReaderContext):

    _log = SpyLog()

    def given_a_415(self):
        self.http.registerEmptyUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20', 415)

    def because_we_fetch_the_stream(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_an_error(self):
        assert(
            any(r.msg == "Received bad http response with status %d from %s"
                and r.levelno == logging.ERROR
                for r in self._log._logs))


class When_we_receive_a_404_range_error(StreamReaderContext):

    _log = SpyLog()

    def given_a_404(self):
        self.http.registerEmptyUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20', 404)
        self.http.registerCallbackUri(
                'http://eventstore.local:2113/streams/newstream/0/forward/20',
                lambda: exec('raise ValueError()'))

    def because_we_fetch_the_stream(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_single_warning(self):
        assert(
            any(r.msg == "Error occurred while requesting %s"
                and r.levelno == logging.WARNING
                for r in self._log._logs))


class When_we_receive_a_408_range_error(StreamReaderContext):

    _log = SpyLog()

    def given_a_408(self):
        self.http.registerEmptyUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20', 408)
        self.http.registerCallbackUri(
                'http://eventstore.local:2113/streams/newstream/0/forward/20',
                lambda: exec('raise ValueError()'))

    def because_we_fetch_the_stream(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_single_warning(self):
        assert(
            any(r.msg == "Error occurred while requesting %s"
                and r.levelno == logging.WARNING
                for r in self._log._logs))


"""
50x range error means that something has gone wonky with event store. Log an error
and retry with a backoff.
"""


class When_we_receive_a_50x_range_error(StreamReaderContext):

    _log = SpyLog()

    def given_a_500(self):
        self.http.registerEmptyUri(
                'http://eventstore.local:2113/streams/newstream/0/forward/20', 500)
        self.http.registerCallbackUri(
                'http://eventstore.local:2113/streams/newstream/0/forward/20',
                lambda: exec('raise ValueError()'))

    def because_we_fetch_the_stream(self):
        self._reader = self.subscribeTo("newstream", -1, nosleep=True)
        with(self._log.capture()):
            mock = self.http.getMock()
            with patch("aiohttp.request", new=mock):
                self._loop.run_until_complete(
                    self._reader.start_consuming()
                )

    def it_should_log_a_single_warning(self):
        assert(
            any(r.msg == "Error occurred while requesting %s"
                and r.levelno == logging.WARNING
                for r in self._log._logs))


class When_an_event_has_bad_json(StreamReaderContext):

    _log = SpyLog()

    def given_a_feed_containing_one_event(self):
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/newstream/0/forward/20',
            SCRIPT_PATH + '/responses/invalid-event.json')

    def because_we_start_the_reader(self):
        with(self._log.capture()):
            self.subscribe_and_run('newstream')

    def it_should_log_an_error(self):
        assert(any(
            r.msg == "Failed to parse json data for %s message %s"
            and r.levelno == logging.ERROR
            for r in self._log._logs))


class When_reading_from_a_stream_with_a_dynamic_date(StreamReaderContext):

    _event = None

    @property
    def the_event(self):
        if(not self._event):
            self._event = self._queue.get_nowait()
        return self._event

    def given_a_feed_for_today_containing_one_event(self):
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/sotd_{}/0/forward/20'.format(
                datetime.date.today().isoformat()),
            '{}/responses/single-event.json'.format(SCRIPT_PATH))

    def because_we_start_the_reader(self):
        self.subscribe_and_run('sotd_#date#')

    def it_should_have_the_event_coming_from_the_stream_for_today(self):
        assert(
            self.the_event.id == UUID('fbf4a1a1-b4a3-4dfe-a01f-ec52c34e16e4'))


class When_reading_from_a_stream_with_a_dynamic_date_twice_across_dates(StreamReaderContext):

    def given_a_dynamic_subscription(self):
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/sotd_2015-08-27/0/forward/20',
            '{}/responses/first_date_response.json'.format(SCRIPT_PATH))
        self.http.registerJsonUri(
            'http://eventstore.local:2113/streams/sotd_2015-08-28/0/forward/20',
            '{}/responses/second_date_response.json'.format(SCRIPT_PATH))

        self.subscribeTo("sotd_#date#", -1, nosleep=True)

    def because_we_start_the_reader_on_two_different_dates(self):
        with freeze_time("2015-08-27"):
            self.run_the_reader()

        with freeze_time("2015-08-28"):
            self.run_the_reader()

        self._first_event = self._queue.get_nowait()
        self._second_event = self._queue.get_nowait()

    def it_should_have_the_event_coming_from_the_stream_for_the_first_date(self):
        assert(
            self._first_event.id == UUID('a51be208-25e5-41b8-a598-ec299a20ef96'))

    def it_should_have_the_event_coming_from_the_stream_for_the_second_date(self):
        assert(
            self._second_event.id == UUID('fbf4a1a1-b4a3-4dfe-a01f-ec52c34e16e4'))
