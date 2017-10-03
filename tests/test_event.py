from atomicpuppy import Event


class When_we_have_an_event:

    def given_an_event(self):
        self.evt = Event(
            id='id',
            type='type',
            data='data',
            stream='stream',
            sequence=12345
        )

    def it_should_have_location(self):
        assert self.evt.location == 'stream/type-12345'

    def it_should_still_have_location(self):
        self.it_should_have_location()
