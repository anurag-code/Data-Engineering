# Copyright 2020 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import main
import os
import json
import pytest


@pytest.fixture
def client():
    main.app.testing = True
    return main.app.test_client()


def test_editor_handler(client):
    os.environ["EDITOR_UPSTREAM_RENDER_URL"] = "http://testing.local"
    r = client.get("/")
    body = r.data.decode()

    assert r.status_code == 200
    assert "<title>Markdown Editor</title>" in body
    assert "This UI allows a user to write Markdown text" in body


def test_render_handler_errors(client):
    r = client.get("/render")
    assert r.status_code == 405

    with pytest.raises(Exception) as e:
        client.post("/render", data="**markdown**")
    assert "Invalid JSON" in str(e.value)


def test_missing_upstream_url(client):
    del os.environ["EDITOR_UPSTREAM_RENDER_URL"]
    with pytest.raises(Exception) as e:
        client.post("/render",
                    data=json.dumps({"data": "**strong text**"}),
                    headers={"Content-Type": "application/json"})
    assert "EDITOR_UPSTREAM_RENDER_URL missing" in str(e.value)
