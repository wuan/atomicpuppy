from collections import namedtuple, defaultdict
import datetime
import logging
import platform

import redis
import yaml

from atomicpuppy.events import RedisCounter


SubscriptionConfig = namedtuple(
    'SubscriptionConfig',
    [
        'streams',
        'counter_factory',
        'instance_name',
        'host',
        'port',
        'timeout',
    ],
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


def make_subscription_config(config_file, counter_instance=None):
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
    ctr = _make_counter(cfg, instance, counter_instance)

    return SubscriptionConfig(
        streams=streams,
        counter_factory=ctr,
        instance_name=instance,
        host=cfg.get("host") or 'localhost',
        port=cfg.get("port") or 2113,
        timeout=cfg.get("timeout") or 20,
    )


def _make_counter(cfg, instance, counter_instance=None):
    if counter_instance:
        return lambda: counter_instance

    ctr = cfg.get('counter')
    if not ctr:
        return (lambda: defaultdict(lambda: -1))
    if ctr["redis"]:
        return lambda: RedisCounter(
            redis.StrictRedis(
                port=ctr["redis"].get("port"),
                host=ctr["redis"].get("host"),
            ),
            instance,
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
            self._logger.info("Creating new SubscriberInfo for {} from event number {}".format(parsed_name, last_read_for_stream))
            subscriber_info = SubscriberInfo(
                parsed_name,
                uri=self._build_uri(
                    parsed_name,
                    last_read_for_stream,
                ),
                last_read=last_read_for_stream,
            )
            self.subscriptions[parsed_name] = subscriber_info

        return self.subscriptions[parsed_name]

    def update_sequence(self, stream_name, sequence):
        # the input stream_name can be the unparsed name
        stream = self.get(stream_name)
        new_subscription = SubscriberInfo(
            stream=stream._stream,
            uri=stream.uri,
            last_read=sequence,
        )
        old_subscription = self.subscriptions[stream._stream]
        assert(old_subscription.last_read < sequence)
        self.subscriptions[stream._stream] = new_subscription

    def update_uri(self, stream_name, uri):
        # the input stream_name can be the unparsed name
        stream = self.get(stream_name)
        new_subscription = SubscriberInfo(
            stream=stream._stream,
            uri=uri,
            last_read=stream.last_read)
        self.subscriptions[stream._stream] = new_subscription

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
