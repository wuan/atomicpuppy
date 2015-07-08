import asyncio
from aiohttp.client import ClientResponse
from unittest.mock import Mock
from collections import deque, defaultdict
import logging


class FakeHttp:

    def __init__(self, loop):
        self._loop = loop
        self._callbacks = defaultdict(deque)

    def registerJsonUris(self, registrations):
        for uri, fn in registrations.items():
            self.registerJsonUri(uri, fn)

    def _make_response(self, uri, filename):
        def cb():

            def read():
                fut = asyncio.Future(loop=self._loop)
                with open(filename) as f:
                    fut.set_result(f.read().encode('utf-8'))
                return fut

            resp = ClientResponse('GET', uri)
            resp.headers = {
                'Content-Type': 'application/json'
            }

            resp.status = 200
            resp.content = Mock()
            resp.content.read.side_effect = read
            resp.close = Mock()
            fut = asyncio.Future(loop=self._loop)
            fut.set_result(resp)
            return fut
        return cb

    def registerJsonUri(self, uri, filename):
        self._callbacks[uri].append(self._make_response(uri, filename))

    def registerJsonsUri(self, uri, filenames):
        for f in filenames:
            self._callbacks[uri].append(self._make_response(uri, f))

    def registerEmptyUri(self, uri, status):
        def cb():
            fut = asyncio.Future(loop=self._loop)
            resp = ClientResponse('GET', 'foo')
            resp.status = status
            fut.set_result(resp)
            return fut
        self._callbacks[uri].append(cb)

    def registerCallbackUri(self, uri, cb):
        self._callbacks[uri].append(cb)

    def registerCallbacksUri(self, uri, funcs):
        for cb in funcs:
            self._callbacks[uri].append(cb)

    @asyncio.coroutine
    def respond(self, uri):
        if(self._callbacks[uri]):
            cb = self._callbacks[uri].popleft()
            return cb()
        print("No response registered for uri "+uri)

    def getMock(self):
        return Mock(side_effect=lambda m, u, **kwargs: self.respond(u))


class SpyLog(logging.StreamHandler):

    def __init__(self):
        self._logs = []
        self.setLevel(logging.DEBUG)
        self.filters = []
        self.lock = None
        self.formatter = logging.Formatter()

    def emit(self, record):
        self.formatter.format(record)
        self._logs.append(record)

    def capture(self):
        return SpyLogContextManager(self)

    @property
    def errors(self):
        return [r for r in self._logs if r.levelname == "ERROR"]

    @property
    def warnings(self):
        return [r for r in self._logs if r.levelname == "WARNING"]


class SpyLogContextManager:

    def __init__(self, log):
        self._log = log

    def __enter__(self):
        logging.getLogger().addHandler(self._log)

    def __exit__(self, type, value, traceback):
        logging.getLogger().removeHandler(self._log)
        print("logs:")
        for r in self._log._logs:
            print(r.msg)
