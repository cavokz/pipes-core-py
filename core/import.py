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

from . import Pipe
from .util import deserialize_yaml, set_field


@Pipe("elastic.pipes.core.import")
def main(pipe, dry_run=False):
    file_name = pipe.config("file", None)
    field = pipe.config("field", None)

    if dry_run:
        return

    if file_name:
        with open(file_name, "r") as f:
            value = deserialize_yaml(f)
    else:
        value = deserialize_yaml(sys.stdin)

    msg_field = f"'{field}'" if field not in (None, "", ".") else "everything"
    msg_file_name = f"'{file_name}'" if file_name else "standard input"
    pipe.logger.info(f"importing {msg_field} from {msg_file_name}...")
    set_field(pipe.state, field, value)
