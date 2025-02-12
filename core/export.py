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

from . import Pipe
from .util import get_field, serialize_yaml


@Pipe("elastic.pipes.core.export")
def main(pipe, dry_run=False):
    file_name = pipe.config("file", None)
    field = pipe.config("field", None)

    if dry_run:
        return

    msg_field = f"'{field}'" if field not in (None, "", ".") else "everything"
    msg_file_name = f"'{file_name}'" if file_name else "standard output"
    pipe.logger.info(f"exporting {msg_field} to {msg_file_name}...")
    value = get_field(pipe.state, field)

    if file_name:
        with open(file_name, "w") as f:
            serialize_yaml(f, value)
    else:
        serialize_yaml(sys.stdout, value)
