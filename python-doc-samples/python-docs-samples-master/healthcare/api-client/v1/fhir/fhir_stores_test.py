# Copyright 2018 Google LLC All Rights Reserved.
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
import sys
import uuid

import backoff
import pytest
from googleapiclient.errors import HttpError
from google.cloud import storage

# Add datasets for bootstrapping datasets for testing
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "datasets"))  # noqa
import datasets
import fhir_stores

cloud_region = "us-central1"
project_id = os.environ["GOOGLE_CLOUD_PROJECT"]

dataset_id = "test_dataset_{}".format(uuid.uuid4())
fhir_store_id = "test_fhir_store-{}".format(uuid.uuid4())

gcs_uri = os.environ["CLOUD_STORAGE_BUCKET"]
RESOURCES = os.path.join(os.path.dirname(__file__), "resources")
source_file_name = "Patient.json"
resource_file = os.path.join(RESOURCES, source_file_name)
import_object = "{}/{}".format(gcs_uri, source_file_name)


@pytest.fixture(scope="module")
def test_dataset():
    @backoff.on_exception(backoff.expo, HttpError, max_time=60)
    def create():
        try:
            datasets.create_dataset(project_id, cloud_region, dataset_id)
        except HttpError as err:
            # We ignore 409 conflict here, because we know it's most
            # likely the first request failed on the client side, but
            # the creation suceeded on the server side.
            if err.resp.status == 409:
                print("Got exception {} while creating dataset".format(err.resp.status))
            else:
                raise

    create()

    yield

    # Clean up
    @backoff.on_exception(backoff.expo, HttpError, max_time=60)
    def clean_up():
        try:
            datasets.delete_dataset(project_id, cloud_region, dataset_id)
        except HttpError as err:
            # The API returns 403 when the dataset doesn't exist.
            if err.resp.status == 404 or err.resp.status == 403:
                print("Got exception {} while deleting dataset".format(err.resp.status))
            else:
                raise

    clean_up()


@pytest.fixture(scope="module")
def test_fhir_store():
    @backoff.on_exception(backoff.expo, HttpError, max_time=60)
    def create():
        try:
            fhir_stores.create_fhir_store(
                project_id, cloud_region, dataset_id, fhir_store_id
            )
        except HttpError as err:
            # We ignore 409 conflict here, because we know it's most
            # likely the first request failed on the client side, but
            # the creation suceeded on the server side.
            if err.resp.status == 409:
                print(
                    "Got exception {} while creating FHIR store".format(err.resp.status)
                )
            else:
                raise

    create()

    yield

    # Clean up
    @backoff.on_exception(backoff.expo, HttpError, max_time=60)
    def clean_up():
        try:
            fhir_stores.delete_fhir_store(
                project_id, cloud_region, dataset_id, fhir_store_id
            )
        except HttpError as err:
            # The API returns 404 when the FHIR store doesn't exist.
            # The API returns 403 when the dataset doesn't exist, so
            # if we try to delete a FHIR store when the parent dataset
            # doesn't exist, the server will return a 403.
            if err.resp.status == 404 or err.resp.status == 403:
                print(
                    "Got exception {} while deleting FHIR store".format(err.resp.status)
                )
            else:
                raise

    clean_up()


@pytest.fixture(scope="module")
def crud_fhir_store_id():
    yield fhir_store_id

    # Clean up
    @backoff.on_exception(backoff.expo, HttpError, max_time=60)
    def clean_up():
        try:
            fhir_stores.delete_fhir_store(
                project_id, cloud_region, dataset_id, fhir_store_id
            )
        except HttpError as err:
            # The API returns 404 when the FHIR store doesn't exist.
            # The API returns 403 when the dataset doesn't exist, so
            # if we try to delete a FHIR store when the parent dataset
            # doesn't exist, the server will return a 403.
            if err.resp.status == 404 or err.resp.status == 403:
                print(
                    "Got exception {} while deleting FHIR store".format(err.resp.status)
                )
            else:
                raise

    clean_up()


@pytest.fixture(scope="module")
def blob():
    @backoff.on_exception(backoff.expo, HttpError, max_time=60)
    def create():
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(gcs_uri)
            blob = bucket.blob(source_file_name)

            blob.upload_from_filename(resource_file)
        except HttpError as err:
            # Ignore 409 errors which are likely caused by
            # the create going through on the server side but
            # failing on the client.
            if err.resp.status == 409:
                print("Got exception {} while creating dataset".format(err.resp.status))
            else:
                raise

    create()

    yield

    # Clean up
    @backoff.on_exception(backoff.expo, HttpError, max_time=60)
    def clean_up():
        try:
            blob.delete()
        except HttpError as err:
            if err.resp.status == 404:
                print(
                    "Got exception {} while deleting blob. Most likely the blob doesn't exist.".format(
                        err.resp.status
                    )
                )
            else:
                raise


def test_crud_fhir_store(test_dataset, capsys):
    fhir_stores.create_fhir_store(project_id, cloud_region, dataset_id, fhir_store_id)

    fhir_stores.get_fhir_store(project_id, cloud_region, dataset_id, fhir_store_id)

    fhir_stores.list_fhir_stores(project_id, cloud_region, dataset_id)

    fhir_stores.delete_fhir_store(project_id, cloud_region, dataset_id, fhir_store_id)

    out, _ = capsys.readouterr()

    # Check that create/get/list/delete worked
    assert "Created FHIR store" in out
    assert "name" in out
    assert "fhirStores" in out
    assert "Deleted FHIR store" in out


def test_get_fhir_store_metadata(test_dataset, test_fhir_store, capsys):
    fhir_stores.get_fhir_store_metadata(
        project_id, cloud_region, dataset_id, fhir_store_id
    )

    out, _ = capsys.readouterr()

    assert "version" in out


def test_patch_fhir_store(test_dataset, test_fhir_store, capsys):
    fhir_stores.patch_fhir_store(project_id, cloud_region, dataset_id, fhir_store_id)

    out, _ = capsys.readouterr()

    assert "Patched FHIR store" in out


def test_import_fhir_store_gcs(test_dataset, test_fhir_store, blob, capsys):
    fhir_stores.import_fhir_resources(
        project_id, cloud_region, dataset_id, fhir_store_id, import_object,
    )

    out, _ = capsys.readouterr()
    assert "Imported FHIR resources" in out


def test_export_fhir_store_gcs(test_dataset, test_fhir_store, capsys):
    fhir_stores.export_fhir_store_gcs(
        project_id, cloud_region, dataset_id, fhir_store_id, gcs_uri,
    )

    out, _ = capsys.readouterr()

    assert "Exported FHIR resources to bucket" in out


def test_get_set_fhir_store_iam_policy(test_dataset, test_fhir_store, capsys):
    get_response = fhir_stores.get_fhir_store_iam_policy(
        project_id, cloud_region, dataset_id, fhir_store_id
    )

    set_response = fhir_stores.set_fhir_store_iam_policy(
        project_id,
        cloud_region,
        dataset_id,
        fhir_store_id,
        "serviceAccount:python-docs-samples-tests@appspot.gserviceaccount.com",
        "roles/viewer",
    )

    out, _ = capsys.readouterr()

    assert "etag" in get_response
    assert "bindings" in set_response
    assert len(set_response["bindings"]) == 1
    assert "python-docs-samples-tests" in str(set_response["bindings"])
    assert "roles/viewer" in str(set_response["bindings"])
