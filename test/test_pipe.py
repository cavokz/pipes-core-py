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

import pytest
from typing_extensions import Annotated, Any, get_args
from util import logger

from core import Pipe, get_pipes
from core.errors import ConfigError, Error


def run(name, config, state, *, dry_run=False):
    Pipe.find(name).run(config, state, dry_run, logger)


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

    run("test_no_dry_run", {}, {}, dry_run=False)
    assert executions == 1

    # if the pipe function does not have the `dry_run` argument,
    # then it's not executed on dry run
    run("test_no_dry_run", {}, {}, dry_run=True)
    assert executions == 1

    run("test_dry_run_false", {}, {}, dry_run=False)
    assert executions == 2

    run("test_dry_run_true", {}, {}, dry_run=True)
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
        run("test_config", {}, {})

    run("test_config", {"name": "me"}, {})

    msg = re.escape("config node type mismatch: 'int' (expected 'str')")
    with pytest.raises(Error, match=msg):
        run("test_config", {"name": 0}, {})

    run("test_config_any", {"name": 1}, {})

    msg = re.escape("mutable default config values are not allowed: {}")
    with pytest.raises(TypeError, match=msg):
        run("test_config_mutable_default", {}, {})


def test_config_optional():
    @Pipe("test_config_optional")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.Config("name")] = "me",
    ):
        assert name == "me"

    run("test_config_optional", {}, {})


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
        run("test_state", {}, {})

    run("test_state", {}, {"name": "me"})

    msg = re.escape("state node type mismatch: 'int' (expected 'str')")
    with pytest.raises(Error, match=msg):
        run("test_state", {}, {"name": 0})

    run("test_state_any", {}, {"name": 1})

    msg = re.escape("mutable default state values are not allowed: {}")
    with pytest.raises(TypeError, match=msg):
        run("test_state_mutable_default", {}, {})


def test_ctx():
    contexts = []

    class TestContext(Pipe.Context):
        name: Annotated[str, Pipe.Config("name"), "some other annotation"]
        user: Annotated[str, Pipe.State("user.name", mutable=True)]

        def __enter__(self):
            contexts.append("inner")
            return self

        def __exit__(self, *_):
            contexts.remove("inner")

    class TestNestedContext(Pipe.Context):
        inner: TestContext
        other: str

        def __enter__(self):
            contexts.append("outer")
            return self

        def __exit__(self, *_):
            contexts.remove("outer")

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

    @Pipe("test_ctx_managed")
    def _(ctx: TestNestedContext):
        assert contexts == ["inner", "outer"]

    msg = "config node not found: 'name'"
    with pytest.raises(KeyError, match=msg):
        run("test_ctx", {}, {})

    msg = "state node not found: 'user.name'"
    with pytest.raises(KeyError, match=msg):
        run("test_ctx", {"name": "me"}, {})

    msg = "cannot specify both 'name' and 'name@'"
    with pytest.raises(ConfigError, match=msg):
        run("test_ctx", {"name": "me", "name@": "name"}, {})

    msg = re.escape("config node type mismatch: 'int' (expected 'str')")
    with pytest.raises(Error, match=msg):
        run("test_ctx", {"name": 0}, {})

    run("test_ctx", {"name": "me"}, {"user": {"name": "you"}})
    run("test_ctx_nested", {"name": "me"}, {"user": {"name": "you"}})

    msg = "can't set attribute"
    with pytest.raises(AttributeError, match=msg):
        run("test_ctx_set", {"name": 0}, {})

    msg = "can't set attribute"
    with pytest.raises(AttributeError, match=msg):
        run("test_ctx_nested_set", {"name": 0}, {})

    config = {"name": "me"}
    state = {}
    run("test_ctx_set2", config, state)
    assert state == {"user": {"name": "me"}}

    config = {"name": "me"}
    state = {}
    run("test_ctx_nested_set2", config, state)
    assert state == {"user": {"name": "me"}}

    assert not contexts
    run("test_ctx_managed", {}, {})
    assert not contexts


def test_state_optional():
    @Pipe("test_state_optional")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name")] = "me",
    ):
        assert name == "me"

    run("test_state_optional", {}, {})


def test_state_indirect():
    @Pipe("test_state_indirect_me")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name")],
    ):
        assert name == "me"

    run("test_state_indirect_me", {}, {"name": "me"})
    run("test_state_indirect_me", {"name@": "username"}, {"username": "me", "name": "you"})

    @Pipe("test_state_indirect_you")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name", indirect=False)],
    ):
        assert name == "you"

    run("test_state_indirect_you", {"name": "username"}, {"username": "me", "name": "you"})

    @Pipe("test_state_indirect_us")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name", indirect="user")],
    ):
        assert name == "us"

    run("test_state_indirect_us", {}, {"name": "us", "username": "them"})

    @Pipe("test_state_indirect_them")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name", indirect="user")],
    ):
        assert name == "them"

    run("test_state_indirect_them", {"user@": "username"}, {"name": "us", "username": "them"})


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
