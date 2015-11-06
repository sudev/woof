import sys
import os
from setuptools import setup, Command


here = os.path.abspath(os.path.dirname(__file__))

with open('VERSION', 'r') as v:
    __version__ = v.read().rstrip()

with open(os.path.join(here, 'README.md')) as f:
    README = f.read()





setup(
    name="woof",
    version=__version__,

    packages=[
        "woof",
        "kafka",
        "kafka.consumer",
        "kafka.partitioner",
        "kafka.producer",
    ],

    author="Jyotiswarup Raiturkar",
    author_email="jyotisr5@gmail.com",
    description="Messaging library ",
    long_description=README,
    keywords="apache kafka",
    install_requires=['six','gevent','greenlet'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
