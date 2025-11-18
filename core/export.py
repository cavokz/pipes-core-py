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

"""Export data from state to files or stdout.

The export pipe writes state data in YAML, JSON, or NDJSON format to
a file or standard output. File format is auto-detected from extension
or specified explicitly.
"""

import sys
from contextlib import ExitStack
from logging import Logger
from pathlib import Path

from typing_extensions import Annotated, Any

from . import Pipe
from .errors import ConfigError
from .util import serialize


class Ctx(Pipe.Context):
    """Context for export pipe configuration.

    Attributes:
        file_name: Output file path, or None for stdout.
        format: Data format ('yaml', 'json', 'ndjson'), or None to guess from extension.
        state: State node to export, or whole state if None.
    """

    file_name: Annotated[
        str,
        Pipe.Config("file"),
        Pipe.Help("file destination of the data"),
        Pipe.Notes("default: standard output"),
    ] = None
    format: Annotated[
        str,
        Pipe.Config("format"),
        Pipe.Help("output format: yaml, json, or ndjson"),
        Pipe.Notes("default: guessed from file extension, or yaml for stdout"),
    ] = None
    state: Annotated[
        Any,
        Pipe.State(None, indirect="node", mutable=True),
        Pipe.Help("state node containing the source data"),
        Pipe.Notes("default: whole state"),
    ]
    data: Annotated[
        dict,
        Pipe.Config("data", indirect=False),
        Pipe.Help("assembled data to export; keys suffixed with [bold green]@[/bold green] are resolved from state"),
        Pipe.Notes("default: state node referenced by 'node@', or whole state"),
    ] = None
    on_failure: Annotated[
        bool,
        Pipe.Config("on-failure"),
        Pipe.Help("export data only when the pipe script has failed"),
    ] = False

    def __init__(self):
        if self.data is not None and self.get_binding("state").node is not None:
            raise ConfigError("param 'data' and 'node@' cannot both be specified")


@Pipe()
def main(ctx: Ctx, log: Logger, stack: ExitStack, dry_run: bool):
    """Export state data to file or stdout.

    Serializes state data in the specified format. If no format is given,
    guesses from file extension or defaults to YAML.

    Args:
        ctx: Export configuration context.
        log: Logger instance.
        stack: ExitStack used to defer export until failure when configured.
        dry_run: If True, skip actual export.

    Example configuration::

        - elastic.pipes.core.export:
            file: output.json
            node@: results
    """

    format = ctx.format
    if format is None:
        if ctx.file_name:
            format = Path(ctx.file_name).suffix.lower()[1:]
            log.debug(f"export file format guessed from file extension: {format}")
        else:
            format = "yaml"
            log.debug(f"assuming export file format: {format}")

    def _export(exc_type=None, exc_value=None, traceback=None):
        if ctx.on_failure and exc_type is None:
            return

        if ctx.data is not None:
            msg_state = "'data'"
            exported = ctx.data
        else:
            node = ctx.get_binding("state").node
            msg_state = "everything" if node is None else f"'{node}'"
            exported = ctx.state

        msg_file_name = f"'{ctx.file_name}'" if ctx.file_name else "standard output"
        on_failure = " due to script failure" if ctx.on_failure else ""

        if dry_run:
            log.info(f"would export {msg_state} to {msg_file_name}{on_failure}...")
            return

        log.info(f"exporting {msg_state} to {msg_file_name}{on_failure}...")

        if ctx.file_name:
            with Path(ctx.file_name).expanduser().open("w") as f:
                serialize(f, exported, format=format)
        else:
            serialize(sys.stdout, exported, format=format)

    if ctx.on_failure:
        log.info("deferring until any script failure...")
        stack.push(_export)
    else:
        _export()


if __name__ == "__main__":
    main()
