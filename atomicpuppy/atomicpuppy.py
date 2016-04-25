import aiohttp
import asyncio
import datetime
from enum import Enum
from .errors import *
import json
import logging
from collections import namedtuple, defaultdict
import platform
import pybreaker
from retrying import retry
import random
import redis
import requests
from uuid import UUID
import yaml
from concurrent.futures import TimeoutError

SubscriptionConfig = namedtuple('SubscriptionConfig', ['streams',
                                                       'counter_factory',
                                                       'instance_name',
                                                       'host',
                                                       'port',
                                                       'timeout'])


class Event:

    def __init__(self, id, type, data, stream, sequence):
        self.id = id
        self.type = type
        self.data = data
        self.stream = stream
        self.sequence = sequence

    def __str__(self):
        return "{}-{} ({}): {}".format(self.type, self.sequence, self.id, self.data)


class EventCounterCircuitBreaker(pybreaker.CircuitBreakerListener):

    def __init__(self):
        self._log = logging.getLogger(__name__)

    def before_call(self, cb, func, *args, **kwargs):
        "Called before the circuit breaker `cb` calls `func`."
        pass

    def state_change(self, cb, old_state, new_state):
        if(new_state.name == 'open'):
            self._log.error("Opening event counter circuit")
        elif(new_state.name == 'closed'):
            self._log.info("Event counter circuit closed")
        pass

    def failure(self, cb, exc):
        "Called when a function invocation raises a system error."
        pass

    def success(self, cb):
        "Called when a function invocation succeeds."
        pass


redis_circuit = pybreaker.CircuitBreaker(
    fail_max=20, reset_timeout=60, listeners=[
        EventCounterCircuitBreaker()
    ]
)


class SubscriberInfo:

    def __init__(
            self,
            stream,
            uri,
            last_read=-1):
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

    def _make_event(self, e):
        try:
            data = json.loads(e["data"], encoding='UTF-8')
        except KeyError:
            # Eventstore allows events with no `data` to be posted. If that
            # happens, then atomicpuppy doesn't know what to do
            self.logger.warning("No `data` key found on event {}".format(e))
            return None
        except ValueError:
            self.logger.error("Failed to parse json data for %s message %s",
                              e.get("eventType"), e.get("eventId"))
            return None
        type = e["eventType"]
        id = UUID(e["eventId"])
        stream = e["positionStreamId"]
        sequence = e["positionEventNumber"]
        return Event(id, type, data, stream, sequence)


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
        self._state = state.amber
        while(True):
            if(last_time < max_time):
                attempts = attempts + 1
            elif self._state == state.amber:
                self._state = state.red
                self._log.error(
                    "Stream fetcher has failed to connect to eventstore at %s in the last %d attempts",
                    uri,
                    attempts)
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
        self._state = state.green
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



class EventRaiser:

    def __init__(self, queue, counter, callback, loop=None):
        self._queue = queue
        self._callback = callback
        self._counter = counter
        self._loop = loop or asyncio.get_event_loop()
        self._logger = logging.getLogger(__name__)

    def stop(self):
        self._is_running = False
        self._logger.info("Stopping")

    @asyncio.coroutine
    def start(self):
        self._is_running = True
        while(self._loop.is_running() and self._is_running):
            try:
                msg = yield from asyncio.wait_for(self._queue.get(),
                                                  timeout=1,
                                                  loop=self._loop)
                if not msg:
                    continue
                self._callback(msg)
                try:
                    self._counter[msg.stream] = msg.sequence
                except CircuitBreakerError:
                    pass
                except RedisError:
                    self._logger.warn("Failed to persist last read event")
            except RejectedMessageException:
                self._logger.warn("%s message %s was rejected and has not been processed",
                                  msg.type, msg.id)
            except(TimeoutError):
                pass
            except:
                self._logger.exception("Failed to process message %s", msg)


    @asyncio.coroutine
    def consume_events(self):
        self._is_running = True
        while(self._loop.is_running() and self._is_running):
            try:
                msg = yield from asyncio.wait_for(self._queue.get(),
                                                  timeout=1,
                                                  loop=self._loop)
                if not msg:
                    self._is_running = False
                    return
                self._callback(msg)
                try:
                    self._counter[msg.stream] = msg.sequence
                except CircuitBreakerError:
                    pass
                except RedisError:
                    self._logger.warn("Failed to persist last read event")
            except RejectedMessageException:
                self._logger.warn("%s message %s was rejected and has not been processed",
                                  msg.type, msg.id)
            except(TimeoutError):
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

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=60000)
    def __getitem__(self, stream):
        self._logger.debug("Fetching last read event for stream "+stream)
        key = self._key(stream)
        val = self._redis.get(key)
        if(val):
            val = int(val)
            self._logger.info(
                "Last read event for stream %s is %d",
                stream,
                val)
            return val
        return -1

    @redis_circuit
    def __setitem__(self, stream, val):
        key = self._key(stream)
        self._redis.set(key, val)

    def _key(self, stream):
        return "urn:atomicpuppy:"+self._instance_name+":"+stream+":position"


class StreamConfigReader:

    def __init__(self):
        pass

    def read(self, config_file):
        cfg = None
        if isinstance(config_file, dict):
            cfg = config_file.get('atomicpuppy')
        elif isinstance(config_file, str):
            with open(config_file) as file:
                cfg = yaml.load(file).get('atomicpuppy')
        else:
            cfg = yaml.load(config_file).get('atomicpuppy')


        streams = []
        instance = cfg.get('instance') or platform.node()
        for stream in cfg.get("streams"):
            streams.append(stream)
        ctr = self._make_counter(cfg, instance)
        return SubscriptionConfig(streams=streams,
                                  counter_factory=ctr,
                                  instance_name=instance,
                                  host=cfg.get("host") or 'localhost',
                                  port=cfg.get("port") or 2113,
                                  timeout=cfg.get("timeout") or 20)

    def _make_counter(self, cfg, instance):
        ctr = cfg.get('counter')
        if(not ctr):
            return (lambda: defaultdict(lambda: -1))
        if(ctr["redis"]):
            return lambda: RedisCounter(
                redis.StrictRedis(port=ctr["redis"].get("port"),
                                  host=ctr["redis"].get("host")),
                instance)


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
            self._logger.info("Creating new SubscriberInfo for {} from event number {}".format(parsed_name, last_read_for_stream))
            s = SubscriberInfo(parsed_name, uri=self._build_uri(parsed_name, last_read_for_stream), last_read=last_read_for_stream)
            self.subscriptions[parsed_name] = s

        return self.subscriptions[parsed_name]

    def update_sequence(self, stream_name, sequence):
        # the input stream_name can be the unparsed name
        s = self.get(stream_name)
        new_subscription = SubscriberInfo(
            stream=s._stream,
            uri=s.uri,
            last_read=sequence)
        old_subscription = self.subscriptions[s._stream]
        assert(old_subscription.last_read < sequence)
        self.subscriptions[s._stream] = new_subscription

    def update_uri(self, stream_name, uri):
        # the input stream_name can be the unparsed name
        s = self.get(stream_name)
        new_subscription = SubscriberInfo(
            stream=s._stream,
            uri=uri,
            last_read=s.last_read)
        self.subscriptions[s._stream] = new_subscription

    def _build_uri(self, stream_name, last_read_for_stream=-1):
        if last_read_for_stream < 0:
            last_read_for_stream = 0

        return 'http://{}:{}/streams/{}/{}/forward/20'.format(
            self.config.host, self.config.port, stream_name, last_read_for_stream)

    def _parse(self, stream_name):
        if '#date#' not in stream_name:
            return stream_name

        today = datetime.date.today().isoformat()
        return stream_name.replace("#date#", today)


class EventStoreJsonEncoder(json.JSONEncoder):

    def default(self, o):
        try:
            if(isinstance(o, UUID)):
                return str(o)
            return json.JSONEncoder.default(self, o)
        except e:
            logging.error(type(o))


class EventPublisher:

    def __init__(self, host, port):
        self._es_url = '{}:{}'.format(host, port)
        self._logger = logging.getLogger("EventPublisher")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000,
           retry_on_result=lambda r: r.status_code >= 500)
    def post(self, event, correlation_id=None):
        uri = 'http://{}/streams/{}'.format(self._es_url, event.stream)
        headers = {'ES-EventType': str(event.type), 'ES-EventId': str(event.id), 'Content-Type': 'application/json'}
        extra = dict(event.__dict__)
        extra.update(headers)
        extra.update({
            'es_uri': uri,
            'method': 'POST'
        })
        self._logger.info("Posting event {} to {}. Headers: {}".format(event.__dict__, uri, headers), extra=extra)
        if(correlation_id):
            event.data["correlation_id"] = correlation_id
        r = requests.post(
            uri,
            headers=headers,
            data=EventStoreJsonEncoder().encode(event.data),
            timeout=0.5)
        if r.status_code:
            extra.update({
                'status_code': r.status_code
            })
            self._logger.info("Received status code {status_code} from EventStore POST.".format(**extra), extra=extra)
        return r
