from setuptools import setup, find_packages
setup(
    name="AtomicPuppy",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "aiohttp>=0.15.3",
        "asyncio==3.4.3",
        "chardet==2.3.0",
        "gevent==1.0.1",
        "greenlet==0.4.5",
        "pybreaker==0.2.3",
        "PyYAML==3.11",
        "redis==2.10.3",
        "retrying==1.3.3",
        "requests==2.7.0"
    ],
    url = 'https://github.com/madedotcom/atomicpuppy',
    download_url = 'https://github.com/madedotcom/atomicpuppy/tarball/0.1',
    description = 'A service-activator component for eventstore',
    author = 'Bob Gregory',
    keywords = ['eventstore'],
)
