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
from logging import Logger
from pathlib import Path

from typing_extensions import Annotated, Any

from . import Pipe
from .util import deserialize, fatal, warn_interactive


class Ctx(Pipe.Context):
    base_dir: Annotated[str, Pipe.State("runtime.base-dir")] = Path.cwd()
    file_name: Annotated[str, Pipe.Config("file")] = None
    format: Annotated[str, Pipe.Config("format")] = None
    state: Annotated[Any, Pipe.State(None, indirect="node", mutable=True)]
    interactive: Annotated[bool, Pipe.Config("interactive")] = False


@Pipe("elastic.pipes.core.import")
def main(ctx: Ctx, log: Logger, dry_run: bool):
    format = ctx.format
    if format is None:
        if ctx.file_name:
            format = Path(ctx.file_name).suffix.lower()[1:]
            log.debug(f"import file format guessed from file extension: {format}")
        else:
            format = "yaml"
            log.debug(f"assuming import file format: {format}")

    if not ctx.file_name and sys.stdin.isatty() and not ctx.interactive:
        fatal("To use `elastic.pipes.core.import` interactively, set `interactive: true` in its configuration.")

    if dry_run:
        return

    node = ctx.get_binding("state").node
    msg_state = "everything" if node is None else f"'{node}'"
    msg_file_name = f"'{ctx.file_name}'" if ctx.file_name else "standard input"
    log.info(f"importing {msg_state} from {msg_file_name}...")

    if ctx.file_name:
        with open(Path(ctx.base_dir) / ctx.file_name, "r") as f:
            warn_interactive(f)
            ctx.state = deserialize(f, format=format) or {}
    else:
        warn_interactive(sys.stdin)
        ctx.state = deserialize(sys.stdin, format=format) or {}
