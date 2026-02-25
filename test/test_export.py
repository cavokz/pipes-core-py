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
from contextlib import contextmanager
from importlib import import_module

from core.util import deserialize

from .util import run

import_module("core.export")


@contextmanager
def run_export(format_, data):
    from tempfile import NamedTemporaryFile

    filename = None
    try:
        with NamedTemporaryFile(mode="w", delete=False) as f:
            filename = f.name

        config = {
            "file": filename,
            "format": format_,
            "node@": "data",
        }
        state = {"data": data}

        with run("core.export", config, state):
            pass

        with open(filename, "r") as f:
            yield deserialize(f, format=format_)
    finally:
        if filename:
            os.unlink(filename)


def test_export_yaml():
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_export("yaml", data) as data_:
        assert isinstance(data_, list)
        assert data_ == data


def test_export_json():
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_export("json", data) as data_:
        assert isinstance(data_, list)
        assert data_ == data


def test_export_ndjson():
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_export("ndjson", data) as data_:
        assert isinstance(data_, list)
        assert data_ == data
