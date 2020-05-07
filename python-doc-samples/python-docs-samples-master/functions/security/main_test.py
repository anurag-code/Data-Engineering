# Copyright 2020 Google LLC
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
import mock

import main


class Response(object):
    def __init__(self, content=u''):
        self.content = content


@mock.patch("main.requests")
def test_functions_bearer_token_should_run(requestsMock):
    requestsMock.get.side_effect = [
        Response(u'some-token'),
        Response(u'function-done')
    ]

    res = main.calling_function(flask.request)

    second_headers = requestsMock.get.call_args_list[0][1]
    assert second_headers == {'headers': {'Metadata-Flavor': 'Google'}}
    assert res == 'function-done'
