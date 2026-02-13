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

"""Helper functions to test Elastic Pipes."""

import sys
from contextlib import ExitStack, contextmanager

from . import Pipe
from .runner import configure_runtime


@contextmanager
def run(name, config, state, logger, *, arguments=None, environment=None, in_memory_state=False, dry_run=False):
    pipe = Pipe.find(name)
    pipe.check_config(config, logger)

    state = state.copy()
    state["pipes"] = [{name: config}]

    configure_runtime(state, sys.stdin, arguments, environment, logger)
    state["runtime"]["in-memory-state"] = in_memory_state

    with ExitStack() as stack:
        pipe.run(config, state, dry_run, logger, stack)
        yield state
