class Event:

    def __init__(self, id, type, data, stream, sequence):
        self.id = id
        self.type = type
        self.data = data
        self.stream = stream
        self.sequence = sequence


