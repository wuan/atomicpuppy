from uuid import uuid4

import httpretty

from atomicpuppy import Event, EventPublisher


class When_a_message_is_posted:

    stream = str(uuid4())
    event_id = str(uuid4())

    class TestConfig:

        def __init__(self, host, port):
            self.host = host
            self.port = port

    def given_a_publisher(self):
        cfg = When_a_message_is_posted.TestConfig('fakehost', '42')
        self.publisher = EventPublisher(cfg.host, cfg.port)

    @httpretty.activate
    def because_an_event_is_published_on_a_stream(self):
        httpretty.register_uri(
            httpretty.POST,
            "http://fakehost:42/streams/{}".format(self.stream),
            body='{}')

        data = {'foo': 'bar'}
        evt = Event(self.event_id, 'my-event-type', data, self.stream, None)
        self.publisher.post(evt)

    def it_should_be_a_POST(self):
        assert(httpretty.last_request().method == "POST")

    def it_should_have_sent_the_correct_id(self):
        assert(httpretty.last_request().headers['ES-EventId'] == self.event_id)

    def it_should_have_sent_the_correct_type(self):
        assert(httpretty.last_request().headers['ES-EventType'] == 'my-event-type')

    def it_should_have_sent_the_correct_body(self):
        assert(httpretty.last_request().body == b'{"foo": "bar"}')
