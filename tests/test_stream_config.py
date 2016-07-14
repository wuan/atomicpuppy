import io
from atomicpuppy.atomicpuppy import *
from atomicpuppy.atomicpuppy import make_subscription_config
import platform


class When_reading_a_config_file:

    def given_a_config_file(self):
        self._file = io.BytesIO(b"""
                atomicpuppy:
                    host: eventstore.local
                    instance: my_stream_reader
                    port: 999
                    streams:
                        - foo
                        - bar
                        - baz
                        - dynamic_#date#
                    counter:
                        redis:
                            host: localhost
                            port: 1234
                """)

    def because_we_read_the_file(self):
        with self._file as f:
            self.result = make_subscription_config(f)

    def it_should_contain_four_streams(self):
        assert(len(self.result.streams) == 4)

    def it_should_have_the_correct_host(self):
        assert(self.result.host == "eventstore.local")

    def it_should_have_the_correct_port(self):
        assert(self.result.port == 999)

    def it_should_have_the_correct_stream_id(self):
        assert(self.result.streams[0] == "foo")
        assert(self.result.streams[1] == "bar")
        assert(self.result.streams[2] == "baz")
        assert(self.result.streams[3] == "dynamic_#date#")

    def it_should_have_the_correct_instance_name(self):
        assert(self.result.instance_name == "my_stream_reader")


class When_the_host_is_not_specified:

    def given_a_config_file_with_no_host(self):
        self._file = io.BytesIO(b"""
                atomicpuppy:
                    port: 1234
                    streams:
                        - foo
                        - bar
                        - baz
                """)

    def because_we_read_the_file(self):
        with self._file as f:
            self.result = make_subscription_config(f)

    def it_should_contain_three_streams(self):
        assert(len(self.result.streams) == 3)

    def it_should_have_the_correct_host(self):
        assert(self.result.host == "localhost")

    def it_should_have_the_correct_port(self):
        assert(self.result.port == 1234)


class When_the_port_is_not_specified:

    def given_a_config_file_with_no_host(self):
        self._file = io.BytesIO(b"""
                atomicpuppy:
                    streams:
                        - foo
                        - bar
                        - baz
                """)

    def because_we_read_the_file(self):
        with self._file as f:
            self.result = make_subscription_config(f)

    def it_should_contain_three_streams(self):
        assert(len(self.result.streams) == 3)

    def it_should_have_the_correct_port(self):
        assert(self.result.port == 2113)


class When_the_instance_name_is_not_specified:

    def given_a_config_file_with_no_host(self):
        self._file = io.BytesIO(b"""
                atomicpuppy:
                    port: 1234
                    streams:
                        - foo
                        - bar
                        - baz
                """)

    def because_we_read_the_file(self):
        with self._file as f:
            self.result = make_subscription_config(f)

    def it_should_default_to_the_hostname(self):
        assert(self.result.instance_name == platform.node())


class When_the_config_specifies_a_redis_counter:

    def given_a_config_file(self):
        self._file = io.BytesIO(b"""
                atomicpuppy:
                    host: eventstore.local
                    port: 999
                    streams:
                        - foo
                        - bar
                        - baz
                    counter:
                        redis:
                            host: localhost
                            port: 1234
                """)

    def because_we_read_the_file(self):
        with self._file as f:
            result = make_subscription_config(f)
        self.ctr = result.counter_factory()

    def it_should_return_a_redis_counter(self):
        assert(isinstance(self.ctr, RedisCounter))


class CustomCounter(EventCounter):

    def __init__(self):
        self._counter = {}

    def __getitem__(self, stream):
        return self._counter.get(stream, -1)

    def __setitem__(self, stream, val):
        self._counter[stream] = val


class When_we_pass_a_custom_counter_without_counter_config:

    def given_a_config_file(self):
        self._file = io.BytesIO(b"""
                atomicpuppy:
                    host: eventstore.local
                    port: 999
                    streams:
                        - foo
                        - bar
                        - baz
                """)

    def because_we_read_the_file(self):
        with self._file as f:
            result = make_subscription_config(f, CustomCounter())
        self.ctr = result.counter_factory()

    def it_should_return_a_redis_counter(self):
        assert(isinstance(self.ctr, CustomCounter))


class When_we_pass_a_custom_counter_with_counter_config:

    def given_a_config_file(self):
        self._file = io.BytesIO(b"""
                atomicpuppy:
                    host: eventstore.local
                    port: 999
                    streams:
                        - foo
                        - bar
                        - baz
                    counter:
                        redis:
                            host: localhost
                            port: 1234
                """)

    def because_we_read_the_file(self):
        with self._file as f:
            result = make_subscription_config(f, CustomCounter())
        self.ctr = result.counter_factory()

    def it_should_return_a_redis_counter(self):
        assert(isinstance(self.ctr, CustomCounter))
