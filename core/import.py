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

from typing_extensions import Annotated

from . import Pipe
from .util import deserialize, fatal, set_node, warn_interactive


@Pipe("elastic.pipes.core.import")
def main(
    pipe: Pipe,
    log: Logger,
    dry_run: bool = False,
    base_dir: Annotated[str, Pipe.State("runtime.base-dir")] = Path.cwd(),
    file_name: Annotated[str, Pipe.Config("file")] = None,
    node: Annotated[str, Pipe.Config("node")] = None,
    format: Annotated[str, Pipe.Config("format")] = None,
    interactive: Annotated[bool, Pipe.Config("interactive")] = False,
):
    if format is None:
        if file_name:
            format = Path(file_name).suffix.lower()[1:]
            log.debug(f"import file format guessed from file extension: {format}")
        else:
            format = "yaml"
            log.debug(f"assuming import file format: {format}")

    if not file_name and sys.stdin.isatty() and not interactive:
        fatal("To use `elastic.pipes.core.import` interactively, set `interactive: true` in its configuration.")

    if dry_run:
        return

    msg_node = f"'{node}'" if node not in (None, "", ".") else "everything"
    msg_file_name = f"'{file_name}'" if file_name else "standard input"
    log.info(f"importing {msg_node} from {msg_file_name}...")

    if file_name:
        with open(Path(base_dir) / file_name, "r") as f:
            warn_interactive(f)
            value = deserialize(f, format=format) or {}
    else:
        warn_interactive(sys.stdin)
        value = deserialize(sys.stdin, format=format) or {}

    set_node(pipe.state, node, value)
