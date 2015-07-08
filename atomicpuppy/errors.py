class FatalError(Exception):
    pass


class UrlError(FatalError):

    def __init__(self, uri):
        self.uri = uri


class HttpClientError(FatalError):

    def __init__(self, uri, status):
        self.status = status
        self.uri = uri


class HttpServerError(Exception):

    def __init__(self, uri, status):
        self.status = status
        self.uri = uri


class RejectedMessageException(Exception):
    pass
