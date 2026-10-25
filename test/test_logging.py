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

"""Tests for the logging format: set_logging_format/setup_logging_format,
the elastic.pipes pipe's logging.format config node, its scope restriction
to that pipe, and the CLI's priority over it."""

import logging

import ecs_logging
import pytest

from core import Pipe
from core.errors import ConfigError, Error
from core.util import set_logging_format, setup_logging, setup_logging_format


def run(name, config, state):
    from .util import run as run_

    with run_(name, config, state) as _:
        pass


@pytest.fixture(autouse=True)
def _logging_handler():
    logger = logging.getLogger("elastic.pipes")
    saved_handlers = logger.handlers[:]
    saved_overridden = getattr(logger, "format_overridden", False)
    logger.handlers = []
    setup_logging()(None)  # attach a single handler, default text formatter, like the CLI does
    try:
        yield
    finally:
        logger.handlers = saved_handlers
        logger.format_overridden = saved_overridden


def _formatters():
    return [h.formatter for h in logging.getLogger("elastic.pipes").handlers]


def test_set_logging_format_text_and_json():
    set_logging_format("json")
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())

    set_logging_format("TEXT")  # case-insensitive
    assert all(isinstance(f, logging.Formatter) and not isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_set_logging_format_invalid():
    with pytest.raises(ConfigError, match="invalid logging format: 'xml'"):
        set_logging_format("xml")


def test_elastic_pipes_sets_logging_format():
    run("elastic.pipes", {"logging": {"format": "json"}}, {})
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_elastic_pipes_rejects_invalid_logging_format_value():
    msg = r"invalid configuration: pipe 'elastic\.pipes': node 'logging\.format': value 'xml'"
    with pytest.raises(Error, match=msg):
        run("elastic.pipes", {"logging": {"format": "xml"}}, {})


def test_elastic_pipes_rejects_non_string_logging_format_value():
    msg = r"invalid configuration: pipe 'elastic\.pipes': node 'logging\.format': value 'True'"
    with pytest.raises(Error, match=msg):
        run("elastic.pipes", {"logging": {"format": True}}, {})


def test_logging_format_rejected_outside_elastic_pipes():
    @Pipe("test_logging_format_other_pipe")
    def _():
        pass

    msg = "unknown config node: 'logging.format'"
    with pytest.raises(Error, match=msg):
        run("test_logging_format_other_pipe", {"logging": {"format": "json"}}, {})


def test_cli_log_format_takes_priority_over_pipe_config():
    setup_logging_format("text")("json")  # simulate --log-format json
    logger = logging.getLogger("elastic.pipes")
    assert logger.format_overridden is True

    # the pipe config asks for text, but the CLI already decided: it is ignored
    run("elastic.pipes", {"logging": {"format": "text"}}, {})
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_cli_log_format_default_does_not_override():
    setup_logging_format("text")(None)  # --log-format not passed
    logger = logging.getLogger("elastic.pipes")
    assert getattr(logger, "format_overridden", False) is False

    # without a CLI override, the pipe config is free to apply
    run("elastic.pipes", {"logging": {"format": "json"}}, {})
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_elastic_pipes_applies_logging_format_before_min_version_check():
    # the format switch must take effect before any other check in the same
    # pipe invocation raises, so the resulting error is logged in that format
    msg = r"current version is older than minimum version"
    with pytest.raises(Error, match=msg):
        run("elastic.pipes", {"logging": {"format": "json"}, "minimum-version": "99.0.0"}, {})
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())
