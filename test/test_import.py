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

import os
import re
import sys
from contextlib import ExitStack
from importlib import import_module
from types import GeneratorType

import pytest

from core.errors import ConfigError
from core.util import serialize_json, serialize_ndjson, serialize_yaml

import_module("core.import")


def run(name, config, state, stack, *, in_memory_state=False):
    from core import Pipe
    from core.runner import configure_runtime

    from .util import logger

    configure_runtime(state, sys.stdin, None, None, logger)
    state["runtime"]["in-memory-state"] = in_memory_state

    pipe = Pipe.find(name)
    pipe.check_config(config)
    pipe.run(config, state, False, logger, stack)


def test_import_streaming_unsupported():
    pipe_name = "elastic.pipes.core.import"

    config = {
        "interactive": True,
        "streaming": True,
    }

    state = {"pipes": [{pipe_name: config}]}

    msg = "cannot use streaming import in UNIX pipe mode"
    with pytest.raises(ConfigError, match=msg):
        with ExitStack() as stack:
            run(pipe_name, config, state, stack)


def check_file_import(format_, populate, assert_, streaming):
    from tempfile import NamedTemporaryFile

    pipe_name = "elastic.pipes.core.import"

    filename = None
    try:
        with NamedTemporaryFile(mode="w", delete=False) as f:
            filename = f.name
            populate(f)

        config = {
            "file": filename,
            "format": format_,
            "streaming": streaming,
            "node@": "data",
        }
        state = {"pipes": [{pipe_name: config}], "data": {}}

        with ExitStack() as stack:
            run(pipe_name, config, state, stack, in_memory_state=streaming)
            assert_(state["data"])
    finally:
        if filename:
            os.unlink(filename)


def test_import_yaml():
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    def _populate(f):
        serialize_yaml(f, data)

    def _assert(state):
        assert isinstance(state, list)
        assert state == data

    check_file_import("yaml", _populate, _assert, False)

    msg = re.escape("cannot stream yaml (try ndjson)")
    with pytest.raises(ConfigError, match=msg):
        check_file_import("yaml", _populate, None, True)


def test_import_json():
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    def _populate(f):
        serialize_json(f, data)

    def _assert(state):
        assert isinstance(state, list)
        assert state == data

    check_file_import("json", _populate, _assert, False)

    msg = re.escape("cannot stream json (try ndjson)")
    with pytest.raises(ConfigError, match=msg):
        check_file_import("json", _populate, None, True)


def test_import_ndjson():
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    def _populate(f):
        serialize_ndjson(f, data)

    def _assert(state):
        assert isinstance(state, list)
        assert state == data

    def _assert_streaming(state):
        assert isinstance(state, GeneratorType)
        assert list(state) == data

    check_file_import("ndjson", _populate, _assert, False)
    check_file_import("ndjson", _populate, _assert_streaming, True)
