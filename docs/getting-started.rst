Getting Started
===============

Overview
--------

Elastic Pipes Core defines a composition system where components (pipes) execute
in sequence, passing state through a shared dictionary. Each pipe can read
from and write to this state, enabling data flow between components.

Key Concepts
------------

Pipes
~~~~~

Pipes are functions decorated with ``@Pipe`` that process state data.
They declare their configuration and state dependencies using type annotations.

State
~~~~~

A dictionary passed through the pipe sequence. Each pipe can read from
and write to the state. State is modified in place and forwarded to
subsequent pipes.

Configuration
~~~~~~~~~~~~~

Each pipe has its own configuration section in the YAML file. Configuration
values are bound to function parameters using ``Pipe.Config``.

Installation
------------

Install from PyPI:

.. code-block:: bash

   pip install elastic-pipes-core

Or install from source:

.. code-block:: bash

   git clone https://github.com/elastic/pipes-core-py.git
   cd pipes-core-py
   pip install .

Basic Usage
-----------

1. Create a Pipe
~~~~~~~~~~~~~~~~

Create a Python file with your pipe definition:

.. code-block:: python

   # threshold-filter.py
   from elastic.pipes.core import Pipe
   from typing_extensions import Annotated
   import logging

   @Pipe()
   def main(
       input: Annotated[list, Pipe.State("input", mutable=True)],
       output: Annotated[list, Pipe.State("output", mutable=True)],
       log: logging.Logger,
       threshold: Annotated[int, Pipe.Config("threshold")] = 10,
   ):
       """Filter data above threshold."""
       log.info(f"Processing {len(input)} items")
       output.extend(x for x in input if x > threshold)

   if __name__ == "__main__":
       main()

``@Pipe()`` only registers the pipe and returns a CLI wrapper; it doesn't
invoke it. The ``if __name__ == "__main__":`` guard is what actually runs it
when the file is executed directly.

With ``@Pipe()`` the pipe name is inferred from the module. In this example,
it is ``threshold-filter``.

``input`` is declared ``mutable=True`` even though it's only read here — see
:doc:`user-guide` for why list/dict state values require this.

2. Create Configuration
~~~~~~~~~~~~~~~~~~~~~~~

Create a YAML configuration file:

.. code-block:: yaml

   # pipeline.yaml
   pipes:
     - elastic.pipes.core.import:
         file: data.json
         node@: input

     - threshold-filter:
         threshold: 50

     - elastic.pipes.core.export:
         file: results.json
         node@: output

   input: []
   output: []

3. Run the Pipeline
~~~~~~~~~~~~~~~~~~~

Execute the pipeline:

.. code-block:: bash

   elastic-pipes run pipeline.yaml

UNIX Pipe Mode
--------------

Individual pipes can run as standalone commands in UNIX pipe mode:

.. code-block:: bash

   printf '%s\n' \
     'pipes:' \
     '  - threshold-filter:' \
     '      threshold: 20' \
     'input: [10, 25, 50]' \
     'output: []' | \
     python threshold-filter.py --pipe-mode

Get help for a pipe:

.. code-block:: bash

   python threshold-filter.py --describe

Next Steps
----------

- Read the :doc:`user-guide` for detailed usage patterns
- See the :doc:`api-reference` for complete API documentation
