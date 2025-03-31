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

"""Elastic Pipes component to export data from the Pipes state."""

import sys
from logging import Logger
from pathlib import Path

from typing_extensions import Annotated

from . import Pipe
from .util import get_node, serialize


@Pipe("elastic.pipes.core.export")
def main(
    pipe: Pipe,
    log: Logger,
    dry_run: bool = False,
    base_dir: Annotated[str, Pipe.State("runtime.base-dir")] = Path.cwd(),
    file_name: Annotated[str, Pipe.Config("file")] = None,
    node: Annotated[str, Pipe.Config("node")] = None,
    format: Annotated[str, Pipe.Config("format")] = None,
):
    if format is None:
        if file_name:
            format = Path(file_name).suffix.lower()[1:]
            log.debug(f"export file format guessed from file extension: {format}")
        else:
            format = "yaml"
            log.debug(f"assuming export file format: {format}")

    if dry_run:
        return

    msg_node = f"'{node}'" if node not in (None, "", ".") else "everything"
    msg_file_name = f"'{file_name}'" if file_name else "standard output"
    log.info(f"exporting {msg_node} to {msg_file_name}...")
    value = get_node(pipe.state, node)

    if file_name:
        with open(Path(base_dir) / file_name, "w") as f:
            serialize(f, value, format=format)
    else:
        serialize(sys.stdout, value, format=format)
