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
import sys
from contextlib import contextmanager
from importlib import import_module

import pytest

from core.util import deserialize

from .util import run

import_module("core.export")

# All (guess, stdio) combinations for export tests
EXPORT_GUESS_STDIO = [
    (True, False),
    (False, True),
    (False, False),
]
EXPORT_GUESS_STDIO_IDS = [f"guess={g},stdio={s}" for g, s in EXPORT_GUESS_STDIO]
export_params = pytest.mark.parametrize("guess, stdio", EXPORT_GUESS_STDIO, ids=EXPORT_GUESS_STDIO_IDS)


@contextmanager
def run_export(format_, data, *, guess, stdio):
    from tempfile import NamedTemporaryFile

    filename = None
    old_stdout = None
    suffix = f".{format_}" if format_ else None
    try:
        config = {"node@": "data"}
        state = {"data": data}

        with NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
            filename = f.name

        if stdio:
            old_stdout = sys.stdout
            sys.stdout = open(filename, "w")
        else:
            config["file"] = filename

        if format_ and not guess:
            config["format"] = format_

        with run("core.export", config, state):
            pass

        sys.stdout.flush()
        with open(filename, "r") as f:
            yield deserialize(f, format=format_)
    finally:
        if old_stdout:
            sys.stdout.close()
            sys.stdout = old_stdout
        if filename:
            os.unlink(filename)


@export_params
def test_export_yaml(guess, stdio):
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_export("yaml", data, guess=guess, stdio=stdio) as data_:
        assert isinstance(data_, list)
        assert data_ == data


@export_params
def test_export_json(guess, stdio):
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_export("json", data, guess=guess, stdio=stdio) as data_:
        assert isinstance(data_, list)
        assert data_ == data


@export_params
def test_export_ndjson(guess, stdio):
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_export("ndjson", data, guess=guess, stdio=stdio) as data_:
        assert isinstance(data_, list)
        assert data_ == data


def test_export_on_failure_no_export_on_success():
    # When on-failure is True, no export should be done on success.
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as f:
        filename = f.name

    try:
        config = {
            "node@": "data",
            "file": filename,
            "on-failure": True,
        }
        data = [{"a": 1}, {"b": 2}]
        state = {"data": data}

        with run("core.export", config, state):
            assert not os.path.exists(filename)

        assert not os.path.exists(filename)
    finally:
        if os.path.exists(filename):
            os.unlink(filename)


def test_export_on_failure_export_on_failure():
    # When on-failure is True, an export should be done on failure.
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as f:
        filename = f.name

    try:
        config = {
            "node@": "data",
            "file": filename,
            "on-failure": True,
        }
        data = [{"a": 1}, {"b": 2}]
        state = {"data": data}

        with pytest.raises(ValueError, match="simulated failure"):
            with run("core.export", config, state):
                assert not os.path.exists(filename)
                raise ValueError("simulated failure")

        with open(filename, "r") as f:
            assert deserialize(f, format="yaml") == data
    finally:
        if os.path.exists(filename):
            os.unlink(filename)
