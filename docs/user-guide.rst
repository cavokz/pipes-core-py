User Guide
==========

This guide covers common usage patterns and advanced features of Elastic Pipes Core.

Writing Pipes
-------------

Basic Pipe Structure
~~~~~~~~~~~~~~~~~~~~

A pipe is a function decorated with ``@Pipe``:

.. code-block:: python

   from elastic.pipes.core import Pipe
   from typing_extensions import Annotated
   import logging

   @Pipe("my.example")
   def example(
       config_param: Annotated[str, Pipe.Config("param")],
       data: Annotated[list, Pipe.State("data", mutable=True)],
       output: Annotated[list, Pipe.State("output", mutable=True)],
       log: logging.Logger,
   ):
       """Process data from state."""
       log.info(f"Processing with {config_param}")
       output.extend(data)

Parameter Binding
~~~~~~~~~~~~~~~~~

Pipe.Config
^^^^^^^^^^^

Bind parameters to configuration nodes:

.. code-block:: python

   threshold: Annotated[int, Pipe.Config("threshold")]

Configuration in YAML:

.. code-block:: yaml

   pipes:
     - my.pipe:
         threshold: 100

Pipe.State
^^^^^^^^^^

Bind parameters to state nodes:

.. code-block:: python

   # Scalar read (no mutable needed)
   threshold: Annotated[int, Pipe.State("threshold")]

   # List/dict values require mutable=True, even when only reading
   documents: Annotated[list, Pipe.State("documents", mutable=True)]

   # Mutable access: the object can be modified in place (e.g. output.append(...))
   output: Annotated[list, Pipe.State("output", mutable=True)]

   # Whole state
   state: Annotated[dict, Pipe.State(None, mutable=True)]

.. note::

   ``mutable=True`` is required to *receive* a list or dict value from state
   at all, not only to modify one. Once a pipe holds a reference to a mutable
   object, nothing can detect or prevent in-place changes to it, so the
   binding must opt in explicitly to acknowledge that.

   Reassigning a whole new value back to state (``ctx.name = value``) is only
   possible for attributes declared on a :class:`Pipe.Context`, via its
   generated property setter. Plain pipe function parameters have no such
   write-back mechanism — mutable containers can only be changed in place.

Indirect References
^^^^^^^^^^^^^^^^^^^

Configuration can specify which state node to use:

.. code-block:: python

   data: Annotated[list, Pipe.Config("data")]

Configuration:

.. code-block:: yaml

   pipes:
     - my.pipe:
         data@: input.documents  # @ suffix means state reference

Disable Indirection
^^^^^^^^^^^^^^^^^^^^

If a parameter should only accept a literal configuration value, pass
``indirect=False`` to reject the ``@``-suffixed key entirely:

.. code-block:: python

   creds: Annotated[dict, Pipe.Config("creds", indirect=False)]

This works:

.. code-block:: yaml

   pipes:
     - my.pipe:
         creds:
           api_key: secret

This is rejected with ``unknown config node: 'creds@'``:

.. code-block:: yaml

   pipes:
     - my.pipe:
         creds@: some.path

Assembled Dictionaries
^^^^^^^^^^^^^^^^^^^^^^^

A dict-typed ``Pipe.Config`` or ``Pipe.State`` parameter can mix literal
values with values resolved from state, by suffixing individual keys with
``@``:

.. code-block:: python

   creds: Annotated[dict, Pipe.Config("creds", indirect=False)]

.. code-block:: yaml

   pipes:
     - my.pipe:
         creds:
           api_key@: runtime.environment.API_KEY
           email: literal@example.com

The pipe receives:

.. code-block:: python

   {"api_key": "abc123", "email": "literal@example.com"}

Keys ending in ``@`` always resolve against *state* (not config), with the
``@`` stripped from the resulting key. This works the same way for
``Pipe.State`` dict parameters, and is independent from the whole-value
``@`` indirection described earlier in this section: that redirects where
the entire value comes from, while this resolves individual dict entries.

Path Syntax
^^^^^^^^^^^

Node paths are dot-separated segments, e.g. ``runtime.arguments.NAME``. If a
segment itself needs to contain a literal dot, quote it with single or double
quotes:

.. code-block:: python

   service_name: Annotated[str, Pipe.Config('data."service.name"')]

.. code-block:: yaml

   pipes:
     - my.pipe:
         data:
           service.name: checkout

Quoted segments work the same way for indirect references. Note that quoting
is only a Python-side convention for writing a dotted path as a single
string; in YAML, nested keys are just written as regular mapping keys, dots
and all:

.. code-block:: yaml

   pipes:
     - my.pipe:
         data:
           service.name@: other.path

Context Managers
~~~~~~~~~~~~~~~~

Use a context when related bindings belong together, or when assigning a
scalar state value is clearer through a property than through a mutable
container.

.. code-block:: python

   class GreetingContext(Pipe.Context):
       name: Annotated[str, Pipe.Config("name")] = "World"
       greeting: Annotated[str, Pipe.State("greeting", mutable=True)]

   @Pipe("my.greeting")
   def greeting(ctx: GreetingContext):
       ctx.greeting = f"Hello, {ctx.name}!"

Contexts also work well for grouping larger sets of related bindings, such as
service configuration and document collections.

Help and Documentation
~~~~~~~~~~~~~~~~~~~~~~

Add help text and notes to parameters:

.. code-block:: python

   threshold: Annotated[
       int,
       Pipe.Config("threshold"),
       Pipe.Help("minimum value to process"),
       Pipe.Notes("default: 10"),
   ] = 10

Pipeline Configuration
----------------------

Basic Structure
~~~~~~~~~~~~~~~

.. code-block:: yaml

   pipes:
     - pipe.name:
         config: value
     - another.pipe:
         setting: value

Logging Configuration
~~~~~~~~~~~~~~~~~~~~~

Set log level per pipe:

.. code-block:: yaml

   pipes:
     - my.pipe:
         logging:
           level: debug

Runtime Arguments
~~~~~~~~~~~~~~~~~

Pass arguments from command line:

.. code-block:: bash

   elastic-pipes run config.yaml --argument threshold=50 --argument format=json

Access in pipe:

.. code-block:: python

   threshold: Annotated[
       int,
       Pipe.Config("threshold"),
   ]

Configuration with ``threshold@: runtime.arguments.threshold``.

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

Similarly for environment variables:

.. code-block:: python

   api_key: Annotated[
       str,
       Pipe.Config("api-key"),
   ]

Configuration with ``api-key@: runtime.environment.API_KEY``.

Built-in Pipes
--------------

elastic.pipes.core.import
~~~~~~~~~~~~~~~~~~~~~~~~~

Import data from files:

.. code-block:: yaml

   - elastic.pipes.core.import:
       file: input.json
       node@: data
       format: json

Streaming support for NDJSON. The destination node must exist beforehand and
must be targeted explicitly with ``node@``, since the streamed value is a
generator and can't replace the whole state (which must be a mapping):

.. code-block:: yaml

   pipes:
     - elastic.pipes.core.import:
         file: large-dataset.ndjson
         node@: documents
         streaming: true

   documents: []

elastic.pipes.core.export
~~~~~~~~~~~~~~~~~~~~~~~~~

Export data to files:

.. code-block:: yaml

   - elastic.pipes.core.export:
       file: output.yaml
       node@: results

Export an assembled dictionary instead of a state node, mixing literal values
with values resolved from state (``data`` and ``node@`` are mutually
exclusive):

.. code-block:: yaml

   - elastic.pipes.core.export:
       file: creds.yaml
       data:
         api_key@: creds.key
         email: literal@example.com

Defer export until failure, exporting only if a later pipe in the sequence
raises an exception, useful for capturing state for debugging without
writing anything on a successful run:

.. code-block:: yaml

   - elastic.pipes.core.export:
       file: failure-state.yaml
       node@: results
       on-failure: true

   - risky.pipe: {}

``on-failure`` only makes sense when this export isn't the last pipe in the
sequence: it defers the actual write until the pipeline's execution scope
exits, and only performs it if that exit was due to an exception from a pipe
that ran afterwards.

Testing Pipes
-------------

Use the test utilities:

.. code-block:: python

   from elastic.pipes.core import test
   import logging

   logger = logging.getLogger("test")

   with test.run(
       "my.pipe",
       {"threshold": 10},
       {"input": [5, 15, 25]},
       logger,
   ) as state:
       assert state["output"] == [15, 25]

Dry Run Mode
------------

.. code-block:: bash

   elastic-pipes run config.yaml --dry-run

``--dry-run`` does not skip execution by itself. Every pipe still runs;
``dry_run`` is only forwarded to pipes that declare a ``dry_run`` parameter,
and it's entirely up to each pipe to honor it:

.. code-block:: python

   @Pipe("my.pipe")
   def process(data: ..., dry_run: bool = False):
       if dry_run:
           return
       write_to_database(data)

A pipe that doesn't declare ``dry_run`` at all runs normally and performs its
full side effects, even with ``--dry-run``. For example, among the built-in
pipes, ``elastic.pipes.core.export`` honors dry-run (it logs what it would
export and skips writing), but ``elastic.pipes.core.import`` does not declare
``dry_run`` and always reads its input and updates state regardless of
``--dry-run``.
