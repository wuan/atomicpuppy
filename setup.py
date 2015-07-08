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
        "retrying==1.3.3"
    ]
)
