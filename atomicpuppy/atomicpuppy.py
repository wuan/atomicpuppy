import datetime
from enum import Enum
import json
import logging
import platform
from uuid import UUID
from concurrent.futures import TimeoutError

from retrying import retry
import yaml

import aiohttp
import asyncio
from atomicpuppy.errors import FatalError,HttpClientError, HttpServerError, RejectedMessageException, UrlError
from collections import namedtuple, defaultdict
import pybreaker
import random
import redis
import requests


SubscriptionConfig = namedtuple(
    'SubscriptionConfig',
    [
        'streams',
        'counter_factory',
        'instance_name',
        'host',
        'port',
        'timeout'
    ]
)


class Event:

    def __init__(self, id, type, data, stream, sequence):
        self.id = id
        self.type = type
        self.data = data
        self.stream = stream
        self.sequence = sequence

    def __str__(self):
        return "{}-{} ({}): {}".format(
            self.type,
            self.sequence,
            self.id,
            self.data
        )


class EventCounterCircuitBreaker(pybreaker.CircuitBreakerListener):

    def __init__(self):
        self._log = logging.getLogger(__name__)

    def before_call(self, callback, func, *args, **kwargs):
        """Called before the circuit breaker `callback` calls `func`."""
        pass

    def state_change(self, callback, old_state, new_state):
        if new_state.name == 'open':
            self._log.error("Opening event counter circuit")
        elif new_state.name == 'closed':
            self._log.info("Event counter circuit closed")
        pass

    def failure(self, callback, exc):
        """Called when a function invocation raises a system error."""
        pass

    def success(self, callback):
        """Called when a function invocation succeeds."""
        pass


redis_circuit = pybreaker.CircuitBreaker(
    fail_max=20,
    reset_timeout=60,
    listeners=[
        EventCounterCircuitBreaker()
    ]
)


class SubscriberInfo:

    def __init__(self, stream, uri, last_read=-1):
        self._stream = stream
        self._uri = uri
        self._last_read = last_read

    @property
    def stream(self):
        return self._stream

    @property
    def last_read(self):
        return self._last_read

    @property
    def uri(self):
        return self._uri


class StreamReader:

    def __init__(self, queue, stream_name, loop, instance_name, subscriptions_store, timeout, nosleep=False):
        self._fetcher = StreamFetcher(
            None,
            loop=loop,
            nosleep=nosleep,
            timeout=timeout
        )
        self._queue = queue
        self._loop = loop
        self._stream = stream_name
        self._subscriptions = subscriptions_store
        self.logger = logging.getLogger(
            'atomicpuppy.stream-reader@{}/{}'.format(
                instance_name,
                stream_name
            )
        )
        self._running = None

    @asyncio.coroutine
    def start_consuming(self, run_once=False):
        """
        Starting a blocking consumption of the stream.

        :param: run_once Set this param to True to consume all the events in the stream and return.
        """
        self._running = True
        while self._running:
            try:
                yield from self._consume()
                if run_once or not self._running:
                    break
                yield from asyncio.sleep(5)
            except UrlError as error:
                self.logger.exception(
                    "Failed to make a request with uri %s",
                    error.uri
                )
                self.stop()
            except HttpClientError as error:
                self.logger.exception(
                    "Received bad http response with status %d from %s",
                    error.status,
                    error.uri
                )
                self.stop()
            except asyncio.CancelledError:
                pass

    def stop(self):
        self._running = False
        self.logger.info("Stopping")
        self._fetcher.stop()

    @asyncio.coroutine
    def _consume(self):
        """
        Start from the last read event and consume all of the remaining events.
        """
        subscription = self._subscriptions.get(self._stream)
        if subscription.last_read > -1:
            self.logger.debug(
                "Last read event is %d - seeking last read on page %s",
                subscription.last_read,
                subscription.uri
            )
            yield from self.read_events_from_page(subscription.uri)
        else:
            response = yield from self._fetcher.fetch(subscription.uri)
            stream_data = yield from response.json()

            last_read_event = self._get_link(stream_data, "last")
            if last_read_event:
                self.logger.debug(
                    "No last read event, skipping to last page %s",
                    last_read_event
                )
                yield from self._walk_from_page(last_read_event)
            else:
                self.logger.debug(
                    "No last read event, yielding events from this page %s",
                    subscription.uri
                )
                yield from self._queue_page_events(stream_data)

    @asyncio.coroutine
    def read_events_from_page(self, uri):
        self.logger.debug("Looking for last read event on page %s", uri)
        response = yield from self._fetcher.fetch(uri)
        stream_data = yield from response.json()
        subscription = self._subscriptions.get(self._stream)
        for entry in stream_data["entries"]:
            if entry["positionEventNumber"] <= subscription.last_read:
                self.logger.debug(
                    "Found last read event on current page, raising events"
                )
                yield from self._queue_page_events(stream_data)
                previous = self._get_link(stream_data, "previous")
                if previous:
                    yield from self._walk_from_page(previous)
                    return

        next_page = self._get_link(stream_data, "next")
        if next_page:
            yield from self.read_events_from_page(next_page)

    @asyncio.coroutine
    def _walk_from_page(self, previous_page_uri):
        current_page_uri = previous_page_uri
        while True:
            self.logger.debug("walking backwards from %s", current_page_uri)
            response = yield from self._fetcher.fetch(current_page_uri)
            stream_data = yield from response.json()
            if stream_data["entries"]:
                self.logger.debug("raising events from page %s", current_page_uri)
                yield from self._queue_page_events(stream_data)
            new_page_uri = self._get_link(stream_data, "previous")
            if not new_page_uri:
                # loop until there's a prev link, otherwise it means
                # we are at the end or we are fetching an empty page
                self.logger.debug(
                    "back-walk completed, new polling uri is %s",
                    current_page_uri
                )
                self._subscriptions.update_uri(self._stream, current_page_uri)
                break

            self.logger.debug("Continuing to previous page %s", current_page_uri)
            current_page_uri = new_page_uri

    @asyncio.coroutine
    def _queue_page_events(self, stream_data):
        """
        This function takes a stream page data, and puts all events on the queue, which later
        will be pushed to the callback.
        """
        subscription = self._subscriptions.get(self._stream)
        stack = []
        for entry in stream_data['entries']:
            if entry["positionEventNumber"] > subscription.last_read:
                stack.append(entry)

        # eventstore returns events starting from the most recent, we need to reverse the order
        stack.reverse()
        for event_data in stack:
            event = self._make_event(event_data)
            if event:
                yield from self._queue.put(event)
                self._subscriptions.update_sequence(self._stream, event.sequence)

    def _get_link(self, stream_data, relation):
        for link in stream_data["links"]:
            if link["relation"] == relation:
                return link["uri"]

    def _make_event(self, event_metadata):
        try:
            event_data = json.loads(event_metadata["data"], encoding='UTF-8')
        except KeyError:
            # Eventstore allows events with no `data` to be posted. If that
            # happens, then atomicpuppy doesn't know what to do
            self.logger.warning("No `data` key found on event {}".format(event_metadata))
            return None
        except ValueError:
            self.logger.error(
                "Failed to parse json data for %s message %s",
                event_metadata.get("eventType"),
                event_metadata.get("eventId")
            )
            return None
        return Event(
            UUID(event_metadata["eventId"]),
            event_metadata["eventType"],
            event_data,
            event_metadata["positionStreamId"],
            event_metadata["positionEventNumber"]
        )


class state(Enum):
    green = 0
    amber = 1
    red = 2


class StreamFetcher:

    def __init__(self, policy, loop, timeout, nosleep=False):
        self._policy = policy
        self._loop = loop
        self._log = logging.getLogger(__name__)
        self._state = state.green
        self._running = True
        self._sleep = None
        self._nosleep = nosleep
        self._exceptions = set()
        self._timeout = timeout

    def stop(self):
        self._running = False
        if self._sleep:
            self._sleep.cancel()

    def sleeps(self, uri):
        max_time = 60
        attempts = 0
        last_time = 0
        self._state = state.amber
        while True:
            if last_time < max_time:
                attempts += 1
            elif self._state == state.amber:
                self._state = state.red
                self._log.error(
                    "Stream fetcher has failed to connect to eventstore at %s in the last %d attempts",
                    uri,
                    attempts
                )
            last_time = 2 ** attempts
            jitter = random.uniform(-0.5, 0.5)
            yield last_time + jitter

    def log(self, exception, uri):
        if type(exception) not in self._exceptions:
            self._log.warn(
                "Error occurred while requesting %s",
                uri,
                exc_info=True
            )
            self._exceptions.add(type(exception))

    @asyncio.coroutine
    def sleep(self, delay):
        if self._nosleep:
            return
        self._sleep = asyncio.futures.Future(loop=self._loop)
        self._sleep._loop.call_later(
            delay,
            self._sleep._set_result_unless_cancelled,
            None
        )
        self._log.debug("retrying fetch in %d seconds", delay)
        yield from self._sleep

    @asyncio.coroutine
    def fetch(self, uri):
        sleep_times = self.sleeps(uri)
        self._exceptions = set()
        self._state = state.green
        for sleep_time in sleep_times:
            try:
                response = yield from asyncio.wait_for(
                    aiohttp.request(
                        'GET',
                        uri,
                        params={"embed": "body"},
                        headers={"Accept": "application/json"},
                        loop=self._loop,
                        connector=aiohttp.TCPConnector(
                            resolve=True,
                            loop=self._loop,
                        )
                    ),
                    self._timeout,
                    loop=self._loop
                )
                if response.status == 200:
                    return response
                if response.status in (404, 408):
                    raise HttpServerError(uri, response.status)
                if 400 <= response.status <= 499:
                    raise HttpClientError(uri, response.status)
                if 500 <= response.status <= 599:
                    raise HttpServerError(uri, response.status)
            except ValueError as exception:
                raise UrlError(exception)
            except (
                aiohttp.errors.ClientError,
                aiohttp.errors.DisconnectedError,
                aiohttp.errors.ClientResponseError,
                HttpServerError,
                TimeoutError
            ) as exception:
                self.log(exception, uri)
                yield from self.sleep(sleep_time)


class EventRaiser:
    """
    EventRaiser will simply keep consuming the non-blocking queue and feeding event to the callback
    provided by the user.

    The queue is supposed to be filled by the StreamReader.
    """

    def __init__(self, queue, counter, callback, loop=None):
        self._queue = queue
        self._callback = callback
        self._counter = counter
        self._loop = loop or asyncio.get_event_loop()
        self._logger = logging.getLogger(__name__)
        self._is_running = None

    def stop(self):
        self._is_running = False
        self._logger.info("Stopping")

    @asyncio.coroutine
    def start(self):
        """
        This method is used to create a on-going process of consuming. It will simply wait and retry
        when it reads all of the events.
        """
        self._is_running = True
        while self._loop.is_running() and self._is_running:
            try:
                message = yield from asyncio.wait_for(
                    self._queue.get(),
                    timeout=1,
                    loop=self._loop
                )
                if not message:
                    continue
                self._callback(message)
                try:
                    self._counter[message.stream] = message.sequence
                except pybreaker.CircuitBreakerError:
                    pass
                except RedisError:
                    self._logger.warn("Failed to persist last read event")
            except RejectedMessageException:
                self._logger.warn(
                    "%s message %s was rejected and has not been processed",
                    message.type,
                    message.id
                )
            except TimeoutError:
                pass
            except:
                self._logger.exception("Failed to process message %s", message)

    @asyncio.coroutine
    def consume_events(self):
        """
        This method is used to consume the event queue once and return.
        """
        self._is_running = True
        while self._loop.is_running() and self._is_running:
            try:
                msg = yield from asyncio.wait_for(
                    self._queue.get(),
                    timeout=1,
                    loop=self._loop
                )
                if not msg:
                    self._is_running = False
                    return
                self._callback(msg)
                try:
                    self._counter[msg.stream] = msg.sequence
                except pybreaker.CircuitBreakerError:
                    pass
                except RedisError:
                    self._logger.warn("Failed to persist last read event")
            except RejectedMessageException:
                self._logger.warn(
                    "%s message %s was rejected and has not been processed",
                    msg.type,
                    msg.id)
            except TimeoutError:
                self._is_running = False
            except:
                self._logger.exception("Failed to process message %s", msg)
                self._is_running = False


class EventCounter:

    def __setitem__(self, stream, event):
        raise NotImplementedError("setitem")

    def __getitem__(self, stream):
        raise NotImplementedError("getitem")


class RedisCounter(EventCounter):

    def __init__(self, redis, instance):
        self._redis = redis
        self._instance_name = instance
        self._logger = logging.getLogger(__name__)

    @retry(
        wait_exponential_multiplier=1000,
        wait_exponential_max=10000,
        stop_max_delay=60000)
    def __getitem__(self, stream):
        self._logger.debug("Fetching last read event for stream " + stream)
        key = self._key(stream)
        value = self._redis.get(key)
        if value:
            value = int(value)
            self._logger.info(
                "Last read event for stream %s is %d",
                stream,
                value
            )
            return value
        return -1

    @redis_circuit
    def __setitem__(self, stream, value):
        key = self._key(stream)
        self._redis.set(key, value)

    def _key(self, stream):
        return "urn:atomicpuppy:" + self._instance_name + ":" + stream + ":position"


class StreamConfigReader:

    def read(self, config_file):
        if isinstance(config_file, dict):
            config = config_file.get('atomicpuppy')
        elif isinstance(config_file, str):
            with open(config_file) as file:
                config = yaml.load(file).get('atomicpuppy')
        else:
            config = yaml.load(config_file).get('atomicpuppy')

        streams = []
        instance = config.get('instance') or platform.node()
        for stream in config.get("streams"):
            streams.append(stream)
        counter = self._make_counter(config, instance)
        return SubscriptionConfig(
            streams=streams,
            counter_factory=counter,
            instance_name=instance,
            host=config.get("host") or 'localhost',
            port=config.get("port") or 2113,
            timeout=config.get("timeout") or 20
        )

    def _make_counter(self, cfg, instance):
        counter = cfg.get('counter')
        if not counter:
            return lambda: defaultdict(lambda: -1)
        if counter["redis"]:
            return lambda: RedisCounter(
                redis.StrictRedis(
                    port=counter["redis"].get("port"),
                    host=counter["redis"].get("host")
                ),
                instance
            )


class SubscriptionInfoStore:
    """
    This class acts like an in-memory buffer of subscriptions.
    It keeps track of subscriptions based on the stream_name
    with dynamic parts replaced.

    It's needed so that new SubscriberInfo objects can be created
    before a consumption when dynamic streams produce a new stream
    name.
    """

    def __init__(self, config, counter):
        self.subscriptions = {}
        self.config = config
        self.counter = counter
        self._logger = logging.getLogger("SubscriptionInfoStore")

    def get(self, stream_name):
        parsed_name = self._parse(stream_name)
        if parsed_name not in self.subscriptions.keys():
            last_read_for_stream = self.counter[parsed_name]
            self._logger.info(
                "Creating new SubscriberInfo for {} from event number {}".format(
                    parsed_name,
                    last_read_for_stream
                )
            )
            self.subscriptions[parsed_name] = SubscriberInfo(
                parsed_name,
                uri=self._build_uri(
                    parsed_name,
                    last_read_for_stream
                ),
                last_read=last_read_for_stream
            )

        return self.subscriptions[parsed_name]

    def update_sequence(self, stream_name, sequence):
        # the input stream_name can be the unparsed name
        subscription = self.get(stream_name)
        subscriber_info = SubscriberInfo(
            stream=subscription._stream,
            uri=subscription.uri,
            last_read=sequence
        )
        old_subscription = self.subscriptions[subscription._stream]
        assert (old_subscription.last_read < sequence)
        self.subscriptions[subscription._stream] = subscriber_info

    def update_uri(self, stream_name, uri):
        # the input stream_name can be the unparsed name
        subscription = self.get(stream_name)
        new_subscription = SubscriberInfo(
            stream=subscription._stream,
            uri=uri,
            last_read=subscription.last_read
        )
        self.subscriptions[subscription._stream] = new_subscription

    def _build_uri(self, stream_name, last_read_for_stream=-1):
        if last_read_for_stream < 0:
            last_read_for_stream = 0

        return 'http://{}:{}/streams/{}/{}/forward/20'.format(
            self.config.host,
            self.config.port,
            stream_name,
            last_read_for_stream
        )

    def _parse(self, stream_name):
        if '#date#' not in stream_name:
            return stream_name

        today = datetime.date.today().isoformat()
        return stream_name.replace("#date#", today)


class EventStoreJsonEncoder(json.JSONEncoder):

    def default(self, object):
        try:
            if isinstance(object, UUID):
                return str(object)
            return json.JSONEncoder.default(self, object)
        except:
            logging.error(type(object))


class EventPublisher:

    def __init__(self, host, port):
        self._es_url = '{}:{}'.format(host, port)
        self._logger = logging.getLogger("EventPublisher")

    @retry(
        wait_exponential_multiplier=1000,
        wait_exponential_max=10000,
        stop_max_delay=30000,
        retry_on_result=lambda r: r.status_code >= 500)
    def post(self, event, correlation_id=None):
        uri = 'http://{}/streams/{}'.format(self._es_url, event.stream)
        headers = {
            'ES-EventType': str(event.type),
            'ES-EventId': str(event.id),
            'Content-Type': 'application/json'
        }
        extra = dict(event.__dict__)
        extra.update(headers)
        extra.update({
            'es_uri': uri,
            'method': 'POST'
        })
        self._logger.info(
            "Posting event {} to {}. Headers: {}".format(
                event.__dict__, uri, headers), extra=extra)
        if correlation_id:
            event.data["correlation_id"] = correlation_id
        response = requests.post(
            uri,
            headers=headers,
            data=EventStoreJsonEncoder().encode(event.data),
            timeout=0.5
        )
        if response.status_code:
            extra.update({
                'status_code': response.status_code
            })
            self._logger.info(
                "Received status code {status_code} from EventStore POST.".format(**extra),
                extra=extra
            )
        return response
