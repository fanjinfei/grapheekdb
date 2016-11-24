# -*- coding:utf-8 -*-

from setuptools import setup, find_packages
from grapheekdb import __version__

setup(
    name="grapheekdb",
    version=__version__,
    author="Raphaël Braud",
    author_email="grapheekdb@gmail.com",
    packages=find_packages(),
    zip_safe=False,
    url="https://bitbucket.org/nidusfr/grapheekdb",
    license="GPL v3",
    description="GrapheekDB is a pure Python graph database which is fast and lightweight",
    long_description=open("README.rst").read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    install_requires=[
        "pyzmq",
        "msgpack-python==0.4.0"
    ],
    entry_points="""
    [console_scripts]
    grapheekserve = grapheekdb.server.serve:main
    """
)
