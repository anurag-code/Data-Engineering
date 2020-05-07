# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import flask
import pytest
import os

import main


# Create a fake "app" for generating test request contexts.
@pytest.fixture(scope="module")
def app():
    return flask.Flask(__name__)


def test_parse_xml(app):
    with app.test_request_context(method='GET', data="<baz>foo</baz>"):
        res = main.parse_xml(flask.request)
        assert res == "{\n  \"baz\": \"foo\"\n}"


def test_parse_multipart_data(app, capsys):
    with app.test_request_context(method='POST', data={'foo': 'bar'}):
        res = main.parse_multipart(flask.request)
        out, _ = capsys.readouterr()
        assert res == 'Done!'
        assert out == 'Processed field: foo\n'


def test_parse_multipart_files(app, capsys):
    with open(__file__, 'rb') as file:
        with app.test_request_context(method='POST', data={'test.py': file}):
            res = main.parse_multipart(flask.request)
            out, _ = capsys.readouterr()
            assert res == 'Done!'
            assert out == 'Processed file: test.py\n'


def test_get_signed_url(app, capsys):
    json = {
        'bucket': os.getenv('GCLOUD_PROJECT'),
        'filename': 'test.txt',
        'contentType': 'text/plain'
    }

    with app.test_request_context(method='POST', json=json):
        url = main.get_signed_url(flask.request)
        assert 'https://storage.googleapis.com/' in url


def test_cors_enabled_function_preflight(app):
    with app.test_request_context(method='OPTIONS'):
        res = main.cors_enabled_function(flask.request)
        assert res[2].get('Access-Control-Allow-Origin') == '*'
        assert res[2].get('Access-Control-Allow-Methods') == 'GET'
        assert res[2].get('Access-Control-Allow-Headers') == 'Content-Type'
        assert res[2].get('Access-Control-Max-Age') == '3600'


def test_cors_enabled_function_main(app):
    with app.test_request_context(method='GET'):
        res = main.cors_enabled_function(flask.request)
        assert res[2].get('Access-Control-Allow-Origin') == '*'


def test_cors_enabled_function_auth_preflight(app):
    with app.test_request_context(method='OPTIONS'):
        res = main.cors_enabled_function_auth(flask.request)
        assert res[2].get('Access-Control-Allow-Origin') == \
            'https://mydomain.com'
        assert res[2].get('Access-Control-Allow-Methods') == 'GET'
        assert res[2].get('Access-Control-Allow-Headers') == 'Authorization'
        assert res[2].get('Access-Control-Max-Age') == '3600'
        assert res[2].get('Access-Control-Allow-Credentials') == 'true'


def test_cors_enabled_function_auth_main(app):
    with app.test_request_context(method='GET'):
        res = main.cors_enabled_function_auth(flask.request)
        assert res[2].get('Access-Control-Allow-Origin') == \
            'https://mydomain.com'
        assert res[2].get('Access-Control-Allow-Credentials') == 'true'
