# Copyright 2018 Google LLC
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
import pytest
import uuid

from googleapiclient import errors
from retrying import retry

import access
import service_accounts

# Setting up variables for testing
GCLOUD_PROJECT = os.environ["GCLOUD_PROJECT"]

# specifying a sample role to be assigned
GCP_ROLE = "roles/owner"


def retry_if_conflict(exception):
    return (isinstance(exception, errors.HttpError)
            and 'There were concurrent policy changes' in str(exception))


@pytest.fixture(scope="module")
def test_member():
    # section to create service account to test policy updates.
    # we use the first portion of uuid4 because full version is too long.
    name = "python-test-" + str(uuid.uuid4()).split('-')[0]
    email = name + "@" + GCLOUD_PROJECT + ".iam.gserviceaccount.com"
    member = "serviceAccount:" + email
    service_accounts.create_service_account(
        GCLOUD_PROJECT, name, "Py Test Account"
    )

    yield member

    # deleting the service account created above
    service_accounts.delete_service_account(email)


def test_get_policy(capsys):
    access.get_policy(GCLOUD_PROJECT, version=3)
    out, _ = capsys.readouterr()
    assert u"etag" in out


def test_modify_policy_add_role(test_member, capsys):
    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
           stop_max_attempt_number=5, retry_on_exception=retry_if_conflict)
    def test_call():
        policy = access.get_policy(GCLOUD_PROJECT, version=3)
        access.modify_policy_add_role(policy, GCLOUD_PROJECT, test_member)
        out, _ = capsys.readouterr()
        assert u"etag" in out
    test_call()


def test_modify_policy_remove_member(test_member, capsys):
    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
           stop_max_attempt_number=5, retry_on_exception=retry_if_conflict)
    def test_call():
        policy = access.get_policy(GCLOUD_PROJECT, version=3)
        access.modify_policy_remove_member(policy, GCP_ROLE, test_member)
        out, _ = capsys.readouterr()
        assert "iam.gserviceaccount.com" in out
    test_call()


def test_set_policy(capsys):
    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
           stop_max_attempt_number=5, retry_on_exception=retry_if_conflict)
    def test_call():
        policy = access.get_policy(GCLOUD_PROJECT, version=3)
        access.set_policy(GCLOUD_PROJECT, policy)
        out, _ = capsys.readouterr()
        assert u"etag" in out
    test_call()


def test_permissions(capsys):
    access.test_permissions(GCLOUD_PROJECT)
    out, _ = capsys.readouterr()
    assert u"permissions" in out
