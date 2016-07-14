import fakeredis
from atomicpuppy.atomicpuppy import RedisCounter


class When_redis_contains_no_information_for_a_stream:

    def given_an_empty_redis_counter(self):
        self.counter = RedisCounter(fakeredis.FakeStrictRedis(), instance="fred")

    def it_should_return_negative_one(self):
        assert(self.counter["my-stream"] == -1)


class When_redis_contains_a_last_read_value_for_a_stream:

    last_read = 92837

    def given_a_redis_instance_with_a_last_read_event(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.redis.set('urn:atomicpuppy:fred:my-stream:position', self.last_read)

    def it_should_return_the_last_read_value(self):
        ctr = RedisCounter(self.redis, instance="fred")
        assert(ctr["my-stream"] == self.last_read)

    def cleanup_redis(self):
        self.redis.flushall()


class When_setting_the_last_read_value:

    def given_an_empty_redis_instance(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.ctr = RedisCounter(self.redis, "instance")

    def because_we_record_an_event(self):
        self.ctr["my-stream"] = 10

    def it_should_be_persisted_in_redis(self):
        assert(self.ctr["my-stream"] == 10)

    def cleanup_redis(self):
        self.redis.flushall()

