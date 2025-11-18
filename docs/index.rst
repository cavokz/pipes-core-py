Elastic Pipes Core Documentation
================================

Elastic Pipes Core is a lightweight Python composition system that executes components
(called "pipes") in sequence, passing state through a shared dictionary.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting-started
   user-guide
   api-reference

Getting Started
---------------

Installation
~~~~~~~~~~~~

Install via pip:

.. code-block:: bash

   pip install elastic-pipes-core

Quick Example
~~~~~~~~~~~~~

Create a simple pipe:

.. code-block:: python

   # hello.py
   from elastic.pipes.core import Pipe
   from typing_extensions import Annotated

   @Pipe()
   def hello(
       output: Annotated[list, Pipe.State("output", mutable=True)],
       name: Annotated[str, Pipe.Config("name")] = "World",
   ):
       output.append(f"Hello, {name}!")

Define a configuration file ``config.yaml`` next to it:

.. code-block:: yaml

   pipes:
     - hello:
         name: ReadTheDocs

   output: []

Run the pipeline:

.. code-block:: bash

   elastic-pipes run config.yaml

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
