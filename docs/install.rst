Installation
============

GrapheekDB is a Python package which is published on PyPI - the Python Package Index.

The simplest way to install it is to use pip, a tool for installing and managing Python packages:

.. code:: bash

    pip install grapheekdb

Or download the archive on PyPI, extract and install it manually with:

.. code:: bash

    python setup.py install

Or checkout Mercurial repository :

.. code:: bash

    hg clone https://bitbucket.org/nidusfr/grapheekdb

Cloning the repository will give you source, examples and documentation.

More on Requirements
--------------------

GrapheekDB uses:

.. code:: bash

    Python 2.7
    zeromq

It has optionnal dependencies (installation depends on your system), those are the packages that may be installed on Ubuntu :

.. code:: bash

    libkyotocabinet-dev
    libkyotocabinet16

When you install GrapheekDB, the latest versions of the required Python dependencies will be pulled out for you.

If you cloned the repository, you can also install them manually using the requirements.txt file that is provided (this will install some optionnal dependencies)

.. code:: bash

    pip install -r requirements.txt

