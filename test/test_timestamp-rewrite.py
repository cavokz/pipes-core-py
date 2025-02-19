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

import re
from datetime import datetime, timedelta
from importlib import import_module
from types import GeneratorType
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies as st

from core.errors import ConfigError
from core.util import parse_timestamp

from .util import run

import_module("core.timestamp-rewrite")
pipe_name = "elastic.pipes.core.timestamp-rewrite"


def timestamp_strategy():
    return st.datetimes(
        min_value=datetime(1970, 1, 1),
        max_value=datetime(2050, 1, 1),
    )


def deltas_strategy():
    return st.lists(
        st.timedeltas(
            min_value=timedelta(milliseconds=1),
            max_value=timedelta(days=1),
        ),
        min_size=1,
        max_size=100,
    ).map(lambda deltas: [delta - timedelta(microseconds=delta.microseconds) for delta in deltas])


def test_timestamp_rewrite_invalid_strategy():
    config = {"strategy-name": "invalid"}
    state = {}

    msg = re.escape("unknown strategy: invalid (allowed strategies: now, now-first)")
    with pytest.raises(ConfigError, match=msg):
        with run(pipe_name, config, state) as _:
            pass


@given(
    doc_count=st.integers(min_value=1, max_value=10),
    base_timestamp=timestamp_strategy(),
)
@patch("core.timestamp-rewrite.datetime")
def test_timestamp_rewrite_now(mock_datetime, doc_count, base_timestamp):
    mock_datetime.now.return_value = base_timestamp
    mock_datetime.strptime = datetime.strptime

    expected_ts = base_timestamp.isoformat(timespec="milliseconds")

    config = {"strategy-name": "now"}

    state = {"documents": [{} for _ in range(doc_count)]}  # list
    with run(pipe_name, config, state) as state_:
        assert len(state_["documents"]) == doc_count
        assert all(doc["@timestamp"] == expected_ts for doc in state_["documents"])

    state = {"documents": ({} for _ in range(doc_count))}  # generator
    with run(pipe_name, config, state) as state_:
        assert isinstance(state_["documents"], GeneratorType)
        assert all(doc["@timestamp"] == expected_ts for doc in state_["documents"])


@given(
    base_timestamp=timestamp_strategy(),
    docs_timestamp=timestamp_strategy(),
    deltas=deltas_strategy(),
)
@patch("core.timestamp-rewrite.datetime")
def test_timestamp_rewrite_now_first(mock_datetime, base_timestamp, docs_timestamp, deltas):
    mock_datetime.now.return_value = base_timestamp
    mock_datetime.strptime = datetime.strptime

    def _timestamp():
        ts = docs_timestamp
        for delta in deltas:
            yield ts.isoformat(timespec="milliseconds")
            ts += delta

    config = {"strategy-name": "now-first"}

    ts = _timestamp()
    state = {"documents": [{"@timestamp": next(ts)} for _ in deltas]}  # list
    with run(pipe_name, config, state) as state_:
        assert len(state_["documents"]) == len(deltas)

        timestamps = [parse_timestamp(doc["@timestamp"]) for doc in state_["documents"]]
        for i, delta in enumerate(deltas):
            if i + 1 == len(timestamps):
                break
            assert timestamps[i + 1] - timestamps[i] == delta

    ts = _timestamp()
    state = {"documents": ({"@timestamp": next(ts)} for _ in deltas)}  # generator
    with run(pipe_name, config, state) as state_:
        assert isinstance(state_["documents"], GeneratorType)

        timestamps = [parse_timestamp(doc["@timestamp"]) for doc in state_["documents"]]
        for i, delta in enumerate(deltas):
            if i + 1 == len(timestamps):
                break
            assert timestamps[i + 1] - timestamps[i] == delta
