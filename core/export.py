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
from pathlib import Path

from . import Pipe
from .util import get_field, serialize


@Pipe("elastic.pipes.core.export")
def main(pipe, dry_run=False):
    file_name = pipe.config("file", None)
    field = pipe.config("field", None)
    format = pipe.config("format", None)

    if format is None:
        if file_name:
            format = Path(file_name).suffix.lower()[1:]
            pipe.logger.debug(f"export file format guessed from file extension: {format}")
        else:
            format = "yaml"
            pipe.logger.debug(f"assuming export file format: {format}")

    if dry_run:
        return

    msg_field = f"'{field}'" if field not in (None, "", ".") else "everything"
    msg_file_name = f"'{file_name}'" if file_name else "standard output"
    pipe.logger.info(f"exporting {msg_field} to {msg_file_name}...")
    value = get_field(pipe.state, field)

    if file_name:
        with open(file_name, "w") as f:
            serialize(f, value, format=format)
    else:
        serialize(sys.stdout, value, format=format)
