[![Build Status](https://travis-ci.org/madedotcom/atomicpuppy.svg?branch=master)](https://travis-ci.org/madedotcom/atomicpuppy)
# atomicpuppy
A service-activator component for eventstore, written in Python


## A Brief and Mostly Useless Quickstart:

```yaml
# config.yaml
atomicpuppy:
    host: localhost
    port: 2113
    # each stream will be polled by a separate async http client
    streams:
        - stream_a
        - stream_b
        - stream_with_a_#date#
    # the counter keeps track of which messages have been processed
    counter:
        redis:
            host: localhost
            port: 6379
```


```python
# main.py
# AtomicPuppy uses asyncio coroutines for concurrent IO
import asyncio
import logging
import signal
from atomicpuppy import AtomicPuppy

# AtomicPuppy needs a callback to pass you messages.
def handle(msg):
  print(msg)

# Config is read from yaml files.
ap = AtomicPuppy('config.yaml', handle)
loop = asyncio.get_event_loop()

# to kill the puppy, call stop()
def stop():
    logging.debug("SIGINT received, shutting down")
    ap.stop()

loop.add_signal_handler(signal.SIGINT, stop)

# and to start it call start.
loop.run_until_complete(ap.start())
```


## Run the tests

A `tox.ini` file is provided to run the tests with different versions of Python.

To run the tests:

1. Make sure you have the Python 3.4 headers installed (on Ubuntu this is `apt-get install python3.4-dev`)
2. Make sure you have the Python 3.5 headers installed (on Ubuntu this is `apt-get install python3.5-dev`)
3. `pip install test-requirements.txt` from the root folder of the repository
4. Run `tox` from the root folder of the repository
