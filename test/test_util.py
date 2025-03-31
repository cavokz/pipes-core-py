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

from core.errors import Error
from core.util import get_node, has_node, set_node, split_path


def test_split_path():
    assert split_path(None) == ()

    for path in ["", ".", ".user", ".user.nick", "user.", "user.nick.", "user..nick"]:
        msg = f"invalid path: {path}"
        with pytest.raises(Error, match=msg):
            _ = split_path(path)

    msg = re.escape("invalid path: type is 'bool' (expected 'str')")
    with pytest.raises(Error, match=msg):
        _ = split_path(True)


def test_has_node():
    d = {"name": "me", "nobody": None}
    assert has_node(d, "name")
    assert not has_node(d, "nobody")
    assert not has_node(d, "user")
    assert not has_node(d, "name.nick")

    assert has_node(d, None)
    assert not has_node({}, None)
    assert not has_node(None, None)


def test_get_node():
    d = {"name": "me", "user": {"name": "you"}, "nobody": None}
    assert get_node(d, "name") == "me"
    assert get_node(d, "user.name") == "you"
    assert get_node(d, "user") == {"name": "you"}
    assert get_node(d, "nick", "me") == "me"
    assert get_node(d, "user.nick", "you") == "you"
    assert get_node(d, "nobody", "us") == "us"
    assert get_node(d, "nobody.else", "them") == "them"

    assert get_node(d, None) is d
    assert get_node(None, None, "where?") == "where?"
    assert get_node(None, "name", "who?") == "who?"

    msg = "'nick'"
    with pytest.raises(KeyError, match=msg):
        _ = get_node(d, "nick")
    msg = "'user.nick'"
    with pytest.raises(KeyError, match=msg):
        _ = get_node(d, "user.nick")
    msg = re.escape("not an object: name (type is str)")
    with pytest.raises(Error, match=msg):
        _ = get_node(d, "name.nick")

    msg = "'nobody'"
    with pytest.raises(KeyError, match=msg):
        _ = get_node(d, "nobody")
    msg = "'nobody.else'"
    with pytest.raises(KeyError, match=msg):
        _ = get_node(d, "nobody.else")

    msg = "None"
    with pytest.raises(KeyError, match=msg):
        _ = get_node(None, None)
    msg = "'nobody'"
    with pytest.raises(KeyError, match=msg):
        _ = get_node(None, "nobody")
    msg = "'nobody.else'"
    with pytest.raises(KeyError, match=msg):
        _ = get_node(None, "nobody.else")


def test_set_node():
    d = {}
    set_node(d, "name", "me")
    assert d == {"name": "me"}
    set_node(d, "user.name", "you")
    assert d == {"name": "me", "user": {"name": "you"}}
    set_node(d, "user.name", "us")
    assert d == {"name": "me", "user": {"name": "us"}}
    set_node(d, "user", None)
    assert d == {"name": "me", "user": None}

    set_node(d, None, {})
    assert d == {}
    set_node(d, None, {"name": "me"})
    assert d == {"name": "me"}
    set_node(d, None, {"user.name": "you"})
    assert d == {"user.name": "you"}
    set_node(d, None, {"name": "me", "user": {"name": "you"}})
    assert d == {"name": "me", "user": {"name": "you"}}

    d2 = get_node(d, "user")
    set_node(d2, None, {"nick": "me"})
    assert d2 == {"nick": "me"}
    assert get_node(d, "user") is d2
    assert d == {"name": "me", "user": {"nick": "me"}}

    msg = re.escape("not an object: name (type is str)")
    with pytest.raises(Error, match=msg):
        set_node(d, "name.nick", "ya")
    msg = "not an object: value type is NoneType"
    with pytest.raises(Error, match=msg):
        set_node(d, None, None)

    msg = re.escape("not an object: None (type is NoneType)")
    with pytest.raises(Error, match=msg):
        set_node(None, None, None)
