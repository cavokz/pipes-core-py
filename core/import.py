# Copyright 2025 Elasticsearch B.V.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Import data from files or stdin into state.

The import pipe reads data in YAML, JSON, or NDJSON format from a file
or standard input and stores it in the state. Supports streaming mode
for NDJSON to process large datasets incrementally.
"""

import sys
from contextlib import ExitStack
from logging import Logger
from pathlib import Path

from typing_extensions import Annotated, Any

from . import Pipe
from .errors import ConfigError
from .util import deserialize, warn_interactive


class Ctx(Pipe.Context):
    """Context for import pipe configuration.

    Attributes:
        file_name: Input file path, or None for stdin.
        format: Data format ('yaml', 'json', 'ndjson'), or None to guess from extension.
        state: State node to import into, or whole state if None.
        interactive: If True, allow reading from terminal stdin.
        streaming: If True, use streaming mode for NDJSON (requires in-memory state).
        in_memory_state: Runtime flag indicating if state is in memory vs UNIX pipe.
    """

    file_name: Annotated[
        str,
        Pipe.Config("file"),
        Pipe.Help("file containing the source data"),
        Pipe.Notes("default: standard input"),
    ] = None
    format: Annotated[
        str,
        Pipe.Config("format"),
        Pipe.Help("input format: yaml, json, or ndjson"),
        Pipe.Notes("default: guessed from file extension, or yaml for stdin"),
    ] = None
    state: Annotated[
        Any,
        Pipe.State(None, indirect="node", mutable=True),
        Pipe.Help("state node destination of the data"),
        Pipe.Notes("default: whole state"),
    ]
    interactive: Annotated[
        bool,
        Pipe.Config("interactive"),
        Pipe.Help("allow importing data from the terminal"),
    ] = False
    streaming: Annotated[
        bool,
        Pipe.Config("streaming"),
        Pipe.Help("allow importing data incrementally"),
    ] = False
    in_memory_state: Annotated[
        bool,
        Pipe.State("runtime.in-memory-state"),
    ] = False

    def __init__(self):
        """Validate import configuration.

        Raises:
            ConfigError: If interactive mode is required but not enabled,
                or if streaming is requested in UNIX pipe mode.
        """
        if not self.file_name and sys.stdin.isatty() and not self.interactive:
            raise ConfigError("to use `elastic.pipes.core.import` interactively, set `interactive: true` in its configuration.")

        if self.streaming and not self.in_memory_state:
            raise ConfigError("cannot use streaming import in UNIX pipe mode")

        if self.format is None:
            if self.file_name:
                self.format = Path(self.file_name).suffix.lower()[1:]
                self.logger.debug(f"import file format guessed from file extension: {self.format}")
            else:
                self.format = "yaml"
                self.logger.debug(f"assuming import file format: {self.format}")


@Pipe()
def main(ctx: Ctx, stack: ExitStack, log: Logger):
    """Import data from file or stdin into state.

    Deserializes data in the specified format and stores it in state.
    If no format is given, guesses from file extension or defaults to YAML.

    Args:
        ctx: Import configuration context.
        stack: ExitStack for file management.
        log: Logger instance.

    Example configuration::

        - elastic.pipes.core.import:
            file: input.json
            node@: documents

        - elastic.pipes.core.import:
            file: large-dataset.ndjson
            node@: documents
            streaming: true

    The destination node must exist beforehand (e.g. ``documents: []``), and
    streaming requires an explicit node@, since the streamed value is a
    generator and can't replace the whole state.
    """

    node = ctx.get_binding("state").node
    msg_state = "everything" if node is None else f"'{node}'"
    msg_file_name = f"'{ctx.file_name}'" if ctx.file_name else "standard input"
    log.info(f"importing {msg_state} from {msg_file_name}...")

    if ctx.file_name:
        f = stack.enter_context(Path(ctx.file_name).expanduser().open("r"))
    else:
        f = sys.stdin

    warn_interactive(f)
    ctx.state = deserialize(f, format=ctx.format, streaming=ctx.streaming) or {}


if __name__ == "__main__":
    main()
