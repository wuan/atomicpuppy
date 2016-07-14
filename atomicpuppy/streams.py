from concurrent.futures import TimeoutError
from uuid import UUID
import aiohttp
import asyncio
import json
import logging
import random
from enum import Enum

from atomicpuppy.errors import HttpClientError, HttpServerError, UrlError
from atomicpuppy.events import Event


class StreamFetcher:

    class state(Enum):
        green = 0
        amber = 1
        red = 2

    def __init__(self, policy, loop, timeout, nosleep=False):
        self._policy = policy
        self._loop = loop
        self._log = logging.getLogger(__name__)
        self._state = self.state.green
        self._running = True
        self._sleep = None
        self._nosleep = nosleep
        self._exns = set()
        self._timeout = timeout

    def stop(self):
        self._running = False
        if(self._sleep):
            self._sleep.cancel()

    def sleeps(self, uri):
        max_time = 60
        attempts = 0
        last_time = 0
        self._state = self.state.amber
        while(True):
            if(last_time < max_time):
                attempts = attempts + 1
            elif self._state == self.state.amber:
                self._state = self.state.red
                self._log.error(
                    "Stream fetcher has failed to connect to eventstore at %s in the last %d attempts",
                    uri,
                    attempts,
                )
            last_time = (2**attempts)
            jitter = random.uniform(-0.5, 0.5)
            yield (last_time) + jitter

    def log(self, e, uri):
        if(type(e) not in self._exns):
            self._log.warn(
                "Error occurred while requesting %s",
                uri,
                exc_info=True)
            self._exns.add(type(e))

    @asyncio.coroutine
    def sleep(self, delay):
        if(self._nosleep):
            return
        self._sleep = asyncio.futures.Future(loop=self._loop)
        self._sleep._loop.call_later(
            delay,
            self._sleep._set_result_unless_cancelled,
            None)
        self._log.debug("retrying fetch in %d seconds", delay)
        yield from self._sleep

    @asyncio.coroutine
    def fetch(self, uri):
        sleep_times = self.sleeps(uri)
        self._exns = set()
        self._state = self.state.green
        for s in sleep_times:
            headers = {"Accept": "application/json"}
            params = {"embed": "body"}
            try:
                response = yield from asyncio.wait_for(
                    aiohttp.request(
                        'GET', uri,
                        params=params,
                        headers=headers,
                        loop=self._loop,
                        connector=aiohttp.TCPConnector(
                            resolve=True,
                            loop=self._loop,
                        )
                    ),
                    self._timeout,
                    loop=self._loop
                )
                if(response.status == 200):
                    return response
                if response.status in (404, 408):
                    raise HttpServerError(uri, response.status)
                if(response.status >= 400 and response.status <= 499):
                    raise HttpClientError(uri, response.status)
                if(response.status >= 500 and response.status <= 599):
                    raise HttpServerError(uri, response.status)
            except ValueError as e:
                raise UrlError(e)
            except (aiohttp.errors.ClientError, aiohttp.errors.DisconnectedError, aiohttp.errors.ClientResponseError) as e:
                self.log(e, uri)
                yield from self.sleep(s)
            except HttpServerError as e:
                self.log(e, uri)
                yield from self.sleep(s)
            except TimeoutError as e:
                self.log(e, uri)
                yield from self.sleep(s)


class StreamReader:

    def __init__(
        self,
        queue,
        stream_name,
        loop,
        instance_name,
        subscriptions_store,
        timeout,
        nosleep=False,
    ):
        self._fetcher = StreamFetcher(None, loop=loop, nosleep=nosleep, timeout=timeout)
        self._queue = queue
        self._loop = loop
        self._stream = stream_name
        self._subscriptions = subscriptions_store
        self.logger = logging.getLogger('atomicpuppy.stream-reader@{}/{}'.format(instance_name, stream_name))

    @asyncio.coroutine
    def start_consuming(self, once=False):
        self._running = True
        while(self._running):
            try:
                yield from self.consume()
                if(once or not self._running):
                    break
                yield from asyncio.sleep(5)
            except UrlError as e:
                self.logger.exception(
                    "Failed to make a request with uri %s",
                    e.uri)
                self.stop()
            except HttpClientError as e:
                self.logger.exception(
                    "Received bad http response with status %d from %s",
                    e.status,
                    e.uri)
                self.stop()
            except asyncio.CancelledError:
                pass

    def stop(self):
        self._running = False
        self.logger.info("Stopping")
        self._fetcher.stop()

    @asyncio.coroutine
    def consume(self):
        subscription = self._subscriptions.get(self._stream)
        if(subscription.last_read > -1):
            self.logger.debug(
                "Last read event is %d - seeking last read on page %s",
                subscription.last_read,
                subscription.uri)
            yield from self.seek_on_page(subscription.uri)
        else:
            r = yield from self._fetcher.fetch(subscription.uri)
            js = yield from r.json()

            last = self._get_link(js, "last")
            if(last):
                self.logger.debug(
                    "No last read event, skipping to last page %s",
                    last)
                yield from self._walk_from_last(last)
            else:
                self.logger.debug(
                    "No last read event, yielding events from this page %s",
                    subscription.uri)
                yield from self._raise_page_events(js)

    @asyncio.coroutine
    def seek_on_page(self, uri):
        self.logger.debug("Looking for last read event on page %s", uri)
        r = yield from self._fetcher.fetch(uri)
        js = yield from r.json()
        yield from self.seek_to_last_read(js)

    @asyncio.coroutine
    def _walk_from_last(self, prev_uri):
        uri = prev_uri
        while True:
            self.logger.debug("walking backwards from %s", uri)
            r = yield from self._fetcher.fetch(uri)
            js = yield from r.json()
            if js["entries"]:
                self.logger.debug("raising events from page %s", uri)
                yield from self._raise_page_events(js)
            prev_uri = self._get_link(js, "previous")
            if not prev_uri:
                # loop until there's a prev link, otherwise it means
                # we are at the end or we are fetching an empty page
                self.logger.debug(
                    "back-walk completed, new polling uri is %s",
                    uri)
                self._subscriptions.update_uri(self._stream, uri)
                break

            self.logger.debug("Continuing to previous page %s", prev_uri)
            uri = prev_uri

    @asyncio.coroutine
    def _raise_page_events(self, js):
        subscription = self._subscriptions.get(self._stream)
        stack = []
        for e in js['entries']:
            if(e["positionEventNumber"] > subscription.last_read):
                stack.append(e)
        while(stack):
            evt = self._make_event(stack.pop())
            if(evt):
                yield from self._queue.put(evt)
                self._subscriptions.update_sequence(self._stream, evt.sequence)

    @asyncio.coroutine
    def seek_to_last_read(self, js):
        subscription = self._subscriptions.get(self._stream)
        for e in js["entries"]:
            if(e["positionEventNumber"] <= subscription.last_read):
                self.logger.debug(
                    "Found last read event on current page, raising events")
                yield from self._raise_page_events(js)
                prev = self._get_link(js, "previous")
                if(prev):
                    yield from self._walk_from_last(prev)
                    return

        nxt = self._get_link(js, "next")
        if(nxt):
            yield from self.seek_on_page(nxt)

    def _get_link(self, js, rel):
        for link in js["links"]:
            if(link["relation"] == rel):
                return link["uri"]

    def _make_event(self, event):
        try:
            data = json.loads(event["data"], encoding='UTF-8')
        except KeyError:
            # Eventstore allows events with no `data` to be posted. If that
            # happens, then atomicpuppy doesn't know what to do
            self.logger.warning("No `data` key found on event {}".format(event))
            return None
        except ValueError:
            self.logger.error("Failed to parse json data for %s message %s", event.get("eventType"), event.get("eventId"))
            return None

        type_ = event["eventType"]
        id_ = UUID(event["eventId"])
        stream = event["positionStreamId"]
        sequence = event["positionEventNumber"]
        return Event(id_, type_, data, stream, sequence)


