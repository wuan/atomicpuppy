from setuptools import setup, find_packages

install_requires = [
    "atomicpuppy==0.3.0",
    "retrying==1.3.3",
]

tests_require = [
    "Contexts==0.10.2",
]

setup(
    name="AtomicPuppy-sqlcounter",
    version="0.0.1",
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    url='https://github.com/madedotcom/atomicpuppy/contribs/sqlcounter',
    description='A sqlalchemy based counter for AtomicPuppy',
    author='Francesco Pighi',
    keywords=['AtomicPuppy'],
    license='MIT',
)
