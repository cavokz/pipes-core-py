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

"""Elastic Pipes component to import data into the Pipes state."""

import sys
from contextlib import ExitStack
from logging import Logger
from pathlib import Path

from typing_extensions import Annotated, Any

from . import Pipe
from .errors import ConfigError
from .util import deserialize, warn_interactive


class Ctx(Pipe.Context):
    file_name: Annotated[
        str,
        Pipe.Config("file"),
        Pipe.Help("file containing the source data"),
        Pipe.Notes("default: standard input"),
    ] = None
    format: Annotated[
        str,
        Pipe.Config("format"),
        Pipe.Help("data format of the file content (ex. yaml, json, ndjson)"),
        Pipe.Notes("default: guessed from the file name extension"),
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
    """Import data from file or standard input."""

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
