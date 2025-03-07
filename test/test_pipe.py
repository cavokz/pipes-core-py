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

import logging
import re

import pytest
from typing_extensions import Annotated, Any, get_args

from core import Pipe, get_pipes
from core.errors import ConfigError, Error

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(name)s - %(message)s"))

logger = logging.getLogger("elastic.pipes")
logger.addHandler(handler)
# logger.setLevel("DEBUG")


def test_dry_run():
    executions = 0

    @Pipe("test_no_dry_run")
    def _():
        nonlocal executions
        executions += 1

    @Pipe("test_dry_run_false")
    def _(dry_run):
        nonlocal executions
        executions += 1
        assert dry_run is False

    @Pipe("test_dry_run_true")
    def _(dry_run):
        nonlocal executions
        executions += 1
        assert dry_run is True

    Pipe.find("test_no_dry_run").run({}, {}, False, logger)
    assert executions == 1

    # if the pipe function does not have the `dry_run` argument,
    # then it's not executed on dry run
    Pipe.find("test_no_dry_run").run({}, {}, True, logger)
    assert executions == 1

    Pipe.find("test_dry_run_false").run({}, {}, False, logger)
    assert executions == 2

    Pipe.find("test_dry_run_true").run({}, {}, True, logger)
    assert executions == 3


def test_multiple():
    @Pipe("test_multiple")
    def _():
        pass

    msg = f"pipe 'test_multiple' is already defined in module '{__name__}'"
    with pytest.raises(ConfigError, match=msg):

        @Pipe("test_multiple")
        def _(pipe):
            pass


def test_config():
    @Pipe("test_config")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.Config("name")],
    ):
        assert name == "me"

    @Pipe("test_config_any")
    def _(
        pipe: Pipe,
        name: Annotated[Any, Pipe.Config("name")],
    ):
        assert name

    @Pipe("test_config_mutable_default")
    def _(
        pipe: Pipe,
        name: Annotated[Any, Pipe.Config("name")] = {},
    ):
        pass

    msg = "config node not found: 'name'"
    with pytest.raises(KeyError, match=msg):
        Pipe.find("test_config").run({}, {}, False, logger)

    Pipe.find("test_config").run({"name": "me"}, {}, False, logger)

    msg = re.escape("config node type mismatch: 'int' (expected 'str')")
    with pytest.raises(Error, match=msg):
        Pipe.find("test_config").run({"name": 0}, {}, False, logger)

    Pipe.find("test_config_any").run({"name": 1}, {}, False, logger)

    msg = re.escape("mutable default config values are not allowed: {}")
    with pytest.raises(TypeError, match=msg):
        Pipe.find("test_config_mutable_default").run({}, {}, False, logger)


def test_config_optional():
    @Pipe("test_config_optional")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.Config("name")] = "me",
    ):
        assert name == "me"

    Pipe.find("test_config_optional").run({}, {}, False, logger)


def test_state():
    @Pipe("test_state")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name")],
    ):
        assert name == "me"

    @Pipe("test_state_any")
    def _(
        pipe: Pipe,
        name: Annotated[Any, Pipe.State("name")],
    ):
        assert name

    @Pipe("test_state_mutable_default")
    def _(
        pipe: Pipe,
        name: Annotated[dict, Pipe.State("name")] = {},
    ):
        pass

    msg = "state node not found: 'name'"
    with pytest.raises(KeyError, match=msg):
        Pipe.find("test_state").run({}, {}, False, logger)

    Pipe.find("test_state").run({}, {"name": "me"}, False, logger)

    msg = re.escape("state node type mismatch: 'int' (expected 'str')")
    with pytest.raises(Error, match=msg):
        Pipe.find("test_state").run({}, {"name": 0}, False, logger)

    Pipe.find("test_state_any").run({}, {"name": 1}, False, logger)

    msg = re.escape("mutable default state values are not allowed: {}")
    with pytest.raises(TypeError, match=msg):
        Pipe.find("test_state_mutable_default").run({}, {}, False, logger)


def test_ctx():
    class TestContext(Pipe.Context):
        name: Annotated[str, Pipe.Config("name"), "some other annotation"]
        user: Annotated[str, Pipe.State("user.name", mutable=True)]

    class TestNestedContext(Pipe.Context):
        inner: TestContext
        other: str

    @Pipe("test_ctx")
    def _(ctx: TestContext):
        assert ctx.name == "me"
        assert ctx.user == "you"
        assert "some other annotation" in get_args(ctx.__annotations__["name"])

    @Pipe("test_ctx_set")
    def _(ctx: TestContext):
        ctx.name = "you"

    @Pipe("test_ctx_set2")
    def _(ctx: TestContext):
        ctx.user = ctx.name

    @Pipe("test_ctx_nested")
    def _(ctx: TestNestedContext):
        assert ctx.inner.name == "me"
        assert ctx.inner.user == "you"
        assert ctx.__annotations__["other"] is str

    @Pipe("test_ctx_nested_set")
    def _(ctx: TestNestedContext):
        ctx.inner.name = "you"

    @Pipe("test_ctx_nested_set2")
    def _(ctx: TestNestedContext):
        ctx.inner.user = ctx.inner.name

    msg = "config node not found: 'name'"
    with pytest.raises(KeyError, match=msg):
        Pipe.find("test_ctx").run({}, {}, False, logger)

    msg = "state node not found: 'user.name'"
    with pytest.raises(KeyError, match=msg):
        Pipe.find("test_ctx").run({"name": "me"}, {}, False, logger)

    msg = "cannot specify both 'name' and 'name@'"
    with pytest.raises(ConfigError, match=msg):
        Pipe.find("test_ctx").run({"name": "me", "name@": "name"}, {}, False, logger)

    msg = re.escape("config node type mismatch: 'int' (expected 'str')")
    with pytest.raises(Error, match=msg):
        Pipe.find("test_ctx").run({"name": 0}, {}, False, logger)

    Pipe.find("test_ctx").run({"name": "me"}, {"user": {"name": "you"}}, False, logger)
    Pipe.find("test_ctx_nested").run({"name": "me"}, {"user": {"name": "you"}}, False, logger)

    msg = "can't set attribute"
    with pytest.raises(AttributeError, match=msg):
        Pipe.find("test_ctx_set").run({"name": 0}, {}, False, logger)

    msg = "can't set attribute"
    with pytest.raises(AttributeError, match=msg):
        Pipe.find("test_ctx_nested_set").run({"name": 0}, {}, False, logger)

    config = {"name": "me"}
    state = {}
    Pipe.find("test_ctx_set2").run(config, state, False, logger)
    assert state == {"user": {"name": "me"}}

    config = {"name": "me"}
    state = {}
    Pipe.find("test_ctx_nested_set2").run(config, state, False, logger)
    assert state == {"user": {"name": "me"}}


def test_state_optional():
    @Pipe("test_state_optional")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name")] = "me",
    ):
        assert name == "me"

    Pipe.find("test_state_optional").run({}, {}, False, logger)


def test_state_indirect():
    @Pipe("test_state_indirect_me")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name")],
    ):
        assert name == "me"

    Pipe.find("test_state_indirect_me").run({}, {"name": "me"}, False, logger)
    Pipe.find("test_state_indirect_me").run({"name@": "username"}, {"username": "me", "name": "you"}, False, logger)

    @Pipe("test_state_indirect_you")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name", indirect=False)],
    ):
        assert name == "you"

    Pipe.find("test_state_indirect_you").run({"name": "username"}, {"username": "me", "name": "you"}, False, logger)

    @Pipe("test_state_indirect_us")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name", indirect="user")],
    ):
        assert name == "us"

    Pipe.find("test_state_indirect_us").run({}, {"name": "us", "username": "them"}, False, logger)

    @Pipe("test_state_indirect_them")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name", indirect="user")],
    ):
        assert name == "them"

    Pipe.find("test_state_indirect_them").run({"user@": "username"}, {"name": "us", "username": "them"}, False, logger)


def test_get_pipes():
    state = None
    pipes = get_pipes(state)
    assert pipes == []

    state = {}
    pipes = get_pipes(state)
    assert pipes == []

    state = {"pipes": None}
    pipes = get_pipes(state)
    assert pipes == []

    state = {"pipes": []}
    pipes = get_pipes(state)
    assert pipes == []

    state = {"pipes": [{"pipe": {}}]}
    pipes = get_pipes(state)
    assert pipes == [("pipe", {})]

    state = {"pipes": [{"pipe": None}]}
    pipes = get_pipes(state)
    assert pipes == [("pipe", {})]

    state = {"pipes": [{"pipe1": {"c1": None}}, {"pipe1": {"c2": None}}, {"pipe2": {"c3": None}}]}
    pipes = get_pipes(state)
    assert pipes == [("pipe1", {"c1": None}), ("pipe1", {"c2": None}), ("pipe2", {"c3": None})]

    msg = re.escape("invalid state: not a mapping: [] (list)")
    with pytest.raises(ConfigError, match=msg):
        _ = get_pipes([])

    msg = re.escape("invalid pipes configuration: not a sequence: {} (dict)")
    with pytest.raises(ConfigError, match=msg):
        state = {"pipes": {}}
        _ = get_pipes(state)

    msg = re.escape("invalid pipe configuration: not a mapping: None (NoneType)")
    with pytest.raises(ConfigError, match=msg):
        state = {"pipes": [None]}
        _ = get_pipes(state)

    msg = re.escape("invalid pipe configuration: multiple pipe names: pipe1, pipe2")
    with pytest.raises(ConfigError, match=msg):
        state = {"pipes": [{"pipe1": None, "pipe2": None}]}
        _ = get_pipes(state)

    msg = re.escape("invalid pipe configuration: not a mapping: [] (list)")
    with pytest.raises(ConfigError, match=msg):
        state = {"pipes": [{"pipe": []}]}
        _ = get_pipes(state)
