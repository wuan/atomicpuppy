import json
import logging

import pybreaker
import requests
import asyncio
from retrying import retry

from atomicpuppy.errors import RejectedMessageException


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
            self.data,
        )


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


redis_circuit = pybreaker.CircuitBreaker( ### IS THIS NECESSARY?
    fail_max=20, reset_timeout=60, listeners=[
        EventCounterCircuitBreaker()
    ]
)


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
                msg = yield from asyncio.wait_for(self._queue.get(), timeout=1, loop=self._loop)
                if not msg:
                    continue
                self._callback(msg)
                try:
                    self._counter[msg.stream] = msg.sequence
                except pybreaker.CircuitBreakerError:
                    pass
                except redis.RedisError:
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
                except pybreaker.CircuitBreakerError:
                    pass
                except redis.RedisError:
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
