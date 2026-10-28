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
to that pipe, the CLI's priority over it, and the top-level exception
handler installed while the format is "json"."""

import logging
import sys
from contextlib import ExitStack

import ecs_logging
import pytest

from core import Pipe
from core.errors import ConfigError, Error
from core.util import (
    fatal,
    log_interrupt,
    set_logging_format,
    setup_logging,
    setup_logging_format,
)


def run(name, config, state):
    from .util import run as run_

    with run_(name, config, state) as _:
        pass


@pytest.fixture(autouse=True)
def _logging_handler():
    logger = logging.getLogger("elastic.pipes")
    saved_handlers = logger.handlers[:]
    saved_overridden = getattr(logger, "format_overridden", False)
    saved_excepthook = sys.excepthook
    logger.handlers = []
    setup_logging()(None)  # attach a single handler, default text formatter, like the CLI does
    try:
        yield
    finally:
        logger.handlers = saved_handlers
        logger.format_overridden = saved_overridden
        sys.excepthook = saved_excepthook


def _formatters():
    return [h.formatter for h in logging.getLogger("elastic.pipes").handlers]


def test_set_logging_format_text_and_json():
    set_logging_format("json")
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())

    set_logging_format("TEXT")  # case-insensitive
    assert all(isinstance(f, logging.Formatter) and not isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_format_before_level_still_wins():
    # click processes given options in command-line order, not declaration
    # order: --log-format may run before --log-level creates the handler
    logging.getLogger("elastic.pipes").handlers = []
    setup_logging_format("text")("json")
    setup_logging("INFO")(None)
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


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


def test_load_pipes_applies_logging_format_before_loading_other_pipes():
    # startup diagnostics (unknown module, missing pipe) must already use
    # the configured format, so this is applied before any pipe is loaded
    from core.runner import load_pipes

    state = {"pipes": [{"elastic.pipes": {"logging": {"format": "json"}}}], "runtime": {"base-dir": "."}}
    load_pipes(state, logging.getLogger("test_load_pipes_logger"))
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_load_pipes_does_not_override_cli_log_format():
    setup_logging_format("text")("json")  # simulate --log-format json

    from core.runner import load_pipes

    state = {"pipes": [{"elastic.pipes": {"logging": {"format": "text"}}}], "runtime": {"base-dir": "."}}
    load_pipes(state, logging.getLogger("test_load_pipes_logger"))
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_elastic_pipes_applies_logging_format_before_min_version_check():
    # format must switch before the min-version check can raise
    msg = r"current version is older than minimum version"
    with pytest.raises(Error, match=msg):
        run("elastic.pipes", {"logging": {"format": "json"}, "minimum-version": "99.0.0"}, {})
    assert all(isinstance(f, ecs_logging.StdlibFormatter) for f in _formatters())


def test_json_format_installs_excepthook():
    original_hook = sys.excepthook

    set_logging_format("json")
    assert sys.excepthook is not original_hook

    set_logging_format("text")
    assert sys.excepthook is original_hook


def test_json_excepthook_logs_a_single_message():
    # elastic.pipes sets propagate=False on this logger; capture directly instead of via caplog
    set_logging_format("json")

    logger = logging.getLogger("elastic.pipes")
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logger.addHandler(handler)

    try:
        try:
            raise ValueError("boom")
        except ValueError:
            exc_info = sys.exc_info()

        sys.excepthook(*exc_info)
    finally:
        logger.removeHandler(handler)

    assert len(records) == 1
    record = records[0]
    assert record.name == "elastic.pipes"
    assert record.levelno == logging.CRITICAL
    assert record.exc_info[0] is ValueError


def test_fatal_logs_through_the_configured_logger_in_json_mode():
    logger = logging.getLogger("elastic.pipes.core")
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logger.addHandler(handler)

    try:
        set_logging_format("json")
        with pytest.raises(SystemExit) as exc_info:
            fatal("boom")
        assert exc_info.value.code == 1
        assert len(records) == 1
        assert records[0].levelno == logging.CRITICAL
        assert records[0].getMessage() == "boom"
    finally:
        logger.removeHandler(handler)


def test_fatal_passes_extra_through_in_json_mode():
    logger = logging.getLogger("elastic.pipes.core")
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logger.addHandler(handler)

    try:
        set_logging_format("json")
        with pytest.raises(SystemExit):
            fatal("boom", extra={"labels": {"pipe": "x"}})
        assert records[0].labels == {"pipe": "x"}
    finally:
        logger.removeHandler(handler)


def test_fatal_unchanged_in_text_mode():
    set_logging_format("text")
    with pytest.raises(SystemExit) as exc_info:
        fatal("boom")
    assert exc_info.value.code == "boom"


def test_log_interrupt_only_in_json():
    @log_interrupt
    def _raise():
        raise KeyboardInterrupt

    logger = logging.getLogger("elastic.pipes.core")
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logger.addHandler(handler)

    try:
        set_logging_format("text")
        with pytest.raises(KeyboardInterrupt):
            _raise()
        assert records == []

        set_logging_format("json")
        with pytest.raises(KeyboardInterrupt):
            _raise()
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
    finally:
        logger.removeHandler(handler)


def test_check_config_and_run_label_debug_logs_with_pipe_name():
    @Pipe("test_lifecycle_labels_pipe")
    def _():
        pass

    pipe = Pipe.find("test_lifecycle_labels_pipe")
    core_logger = logging.getLogger("test_lifecycle_labels_core_logger")
    core_logger.setLevel(logging.DEBUG)
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    core_logger.addHandler(handler)

    try:
        pipe.check_config({}, core_logger)
        with ExitStack() as stack:
            pipe.run({}, {}, False, core_logger, stack)
    finally:
        core_logger.removeHandler(handler)

    assert records
    assert all(r.labels == {"pipe": "test_lifecycle_labels_pipe"} for r in records)
