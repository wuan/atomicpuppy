from setuptools import setup, find_packages

install_requires = [
    "aiohttp>=2.2.3",
    "chardet>=2.3.0",
    "gevent>=1.1.1",
    "greenlet>=0.4.12",
    "pybreaker>=0.2.3",
    "PyYAML>=3.12",
    "redis==2.10.5",
    "retrying>=1.3.3",
    "requests>=2.7.0"
]

tests_require = [
    "Contexts",
    "fakeredis==0.6.1",
    "freezegun==0.3.3",
    "HTTPretty"
]

extras = {
    'test': tests_require,
}

setup(
    name="AtomicPuppy",
    version="0.4.1",
    packages=find_packages(),
    dependency_links=[
        "git+https://github.com/OddBloke/HTTPretty.git@f899d1bda8234658c2cec5aab027cb5b7c42203c#egg=HTTPretty"
        "git+https://github.com/damiansoriano/Contexts.git@4ad390aa4f514eff94b8ef43f6da0b404e42e1a7#egg=Contexts"
    ],
    install_requires=install_requires,
    tests_require=tests_require,
    url='https://github.com/madedotcom/atomicpuppy',
    download_url='https://github.com/madedotcom/atomicpuppy/tarball/0.3.pre',
    description='A service-activator component for eventstore',
    author='Bob Gregory',
    author_email='bob@made.com',
    keywords=['eventstore'],
    license='MIT',
)
