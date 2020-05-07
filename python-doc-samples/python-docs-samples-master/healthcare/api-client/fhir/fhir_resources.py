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

import argparse
import json
import os

from google.auth.transport import requests
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

_BASE_URL = "https://healthcare.googleapis.com/v1beta1"


# [START healthcare_get_session]
def get_session(service_account_json):
    """
    Returns an authorized Requests Session class using the service account
    credentials JSON. This class is used to perform requests to the
    Healthcare API endpoint.
    """

    # Pass in the credentials and project ID. If none supplied, get them
    # from the environment.
    credentials = service_account.Credentials.from_service_account_file(
        service_account_json
    )
    scoped_credentials = credentials.with_scopes(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )

    # Create a requests Session object with the credentials.
    session = requests.AuthorizedSession(scoped_credentials)

    return session


# [END healthcare_get_session]


# [START healthcare_create_resource]
def create_patient(
    service_account_json, base_url, project_id, cloud_region, dataset_id, fhir_store_id
):
    """Creates a new Patient resource in a FHIR store."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    fhir_store_path = "{}/datasets/{}/fhirStores/{}/fhir/Patient".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    body = {
        "name": [{"use": "official", "family": "Smith", "given": ["Darcy"]}],
        "gender": "female",
        "birthDate": "1970-01-01",
        "resourceType": "Patient",
    }

    try:
        response = session.post(fhir_store_path, headers=headers, json=body)
        response.raise_for_status()

        resource = response.json()

        print("Created Patient resource with ID {}".format(resource["id"]))

        return response
    except HttpError as err:
        print(err)
        return ""


# [END healthcare_create_resource]


# [START healthcare_create_encounter]
def create_encounter(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    patient_id,
):
    """Creates a new Encounter resource in a FHIR store based on a Patient."""
    url = "{}/projects/{}/locations/{}".format(
        base_url, project_id, cloud_region, patient_id
    )

    fhir_store_path = "{}/datasets/{}/fhirStores/{}/fhir/Encounter".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    body = {
        "status": "finished",
        "class": {
            "system": "http://hl7.org/fhir/v3/ActCode",
            "code": "IMP",
            "display": "inpatient encounter",
        },
        "reason": [
            {
                "text": "The patient had an abnormal heart rate. She was"
                " concerned about this."
            }
        ],
        "subject": {"reference": "Patient/{}".format(patient_id)},
        "resourceType": "Encounter",
    }

    try:
        response = session.post(fhir_store_path, headers=headers, json=body)
        response.raise_for_status()

        resource = response.json()

        print("Created Encounter resource with ID {}".format(resource["id"]))

        return response
    except HttpError as err:
        print(err)
        return ""


# [END healthcare_create_encounter]


# [START healthcare_create_observation]
def create_observation(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    patient_id,
    encounter_id,
):
    """
    Creates a new Observation resource in a FHIR store based on
    an Encounter.
    """
    url = "{}/projects/{}/locations/{}".format(
        base_url, project_id, cloud_region, patient_id
    )

    fhir_store_path = "{}/datasets/{}/fhirStores/{}/fhir/Observation".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    body = {
        "resourceType": "Observation",
        "identifier": [{"system": "my-code-system", "value": "ABC-12345"}],
        "status": "final",
        "subject": {"reference": "Patient/{}".format(patient_id)},
        "effectiveDateTime": "2019-01-01T00:00:00+00:00",
        "valueQuantity": {"value": 80, "unit": "bpm"},
        "context": {"reference": "Encounter/{}".format(encounter_id)},
    }

    try:
        response = session.post(fhir_store_path, headers=headers, json=body)
        response.raise_for_status()

        resource = response.json()

        print("Created Observation resource with ID {}".format(resource["id"]))

        return response
    except HttpError as err:
        print(err)
        return ""


# [END healthcare_create_observation]


# [START healthcare_delete_resource]
def delete_resource(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
    resource_id,
):
    """
    Deletes a FHIR resource. Regardless of whether the operation succeeds or
    fails, the server returns a 200 OK HTTP status code. To check that the
    resource was successfully deleted, search for or get the resource and
    see if it exists.
    """
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, resource_type, resource_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    response = session.delete(resource_path)
    print("Deleted {} resource with ID {}.".format(resource_type, resource_id))

    return response


# [END healthcare_delete_resource]


# [START healthcare_get_resource]
def get_resource(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
    resource_id,
):
    """Gets a FHIR resource."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, resource_type, resource_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    response = session.get(resource_path, headers=headers)
    response.raise_for_status()

    resource = response.json()

    print("Got {} resource:".format(resource["resourceType"]))
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_get_resource]


# [START healthcare_list_resource_history]
def list_resource_history(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
    resource_id,
):
    """Gets the history of a resource."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, resource_type, resource_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    response = session.get(resource_path + "/_history", headers=headers)
    response.raise_for_status()

    resource = response.json()

    print(
        "History for {} resource:".format(
            resource["entry"][0]["resource"]["resourceType"]
        )
    )
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_list_resource_history]


# [START healthcare_get_resource_history]
def get_resource_history(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
    resource_id,
    version_id,
):
    """Gets a version resource."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, resource_type, resource_id
    )
    resource_path += "/_history/{}".format(version_id)

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    response = session.get(resource_path, headers=headers)
    response.raise_for_status()

    resource = response.json()

    print("Got history for {} resource:".format(resource_type))
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_get_resource_history]


# [START healthcare_export_fhir_resources]
def export_resources(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    gcs_destination,
):
    """Exports resources in a FHIR store."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    body = {"gcsDestination": {"uriPrefix": gcs_destination}}

    response = session.post(resource_path + ":export", headers=headers, json=body)
    response.raise_for_status()

    resource = response.json()

    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_export_fhir_resources]


# [START healthcare_import_fhir_resources]
def import_resources(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    gcs_source,
):
    """Exports resources in a FHIR store."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    body = {
        "gcsSource": {"uriPrefix": gcs_source},
        "gcsErrorDestination": {"uriPrefix": gcs_source + "_errors"},
    }

    response = session.post(resource_path + ":import", headers=headers, json=body)
    response.raise_for_status()

    resource = response.json()

    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_import_fhir_resources]


# [START healthcare_delete_resource_purge]
def delete_resource_purge(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
    resource_id,
):
    """Deletes versions of a resource (excluding current version)."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, resource_type, resource_id
    )
    resource_path += "/$purge"

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    response = session.delete(resource_path, headers=headers)
    response.raise_for_status()

    if response.status_code < 400:
        print(
            "Deleted versions of {} resource "
            "(excluding current version).".format(resource_type)
        )

    return response


# [END healthcare_delete_resource_purge]


# [START healthcare_update_resource]
def update_resource(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
    resource_id,
):
    """Updates an existing resource."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, resource_type, resource_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    body = {"resourceType": resource_type, "active": True, "id": resource_id}

    response = session.put(resource_path, headers=headers, json=body)
    response.raise_for_status()

    resource = response.json()

    print("Updated {} resource:".format(resource["resourceType"]))
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_update_resource]


# [START healthcare_conditional_update_resource]
def conditional_update_resource(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    patient_id,
    encounter_id,
):
    """
    If a resource is found based on the search criteria specified in
    the query parameters, updates the entire contents of that resource.
    """
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    # The search query in this request updates all Observations
    # using the Observation's identifier (ABC-12345 in my-code-system)
    # so that their 'status' is 'cancelled'.
    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/Observation".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    body = {
        "effectiveDateTime": "2019-01-01T00:00:00+00:00",
        "resourceType": "Observation",
        "context": {"reference": "Encounter/{}".format(encounter_id)},
        "identifier": [{"system": "my-code-system", "value": "ABC-12345"}],
        "status": "cancelled",
        "subject": {"reference": "Patient/{}".format(patient_id)},
        "valueQuantity": {"unit": "bpm", "value": 80},
    }

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    params = {"identifier": "my-code-system|ABC-12345"}

    response = session.put(resource_path, headers=headers, params=params, json=body)

    response.raise_for_status()
    resource = response.json()

    print(
        "Conditionally updated Observations with the identifier "
        "'my-code-system|ABC-12345' to have a 'status' of "
        "'cancelled'."
    )
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_conditional_update_resource]


# [START healthcare_conditional_delete_resource]
def conditional_delete_resource(
    service_account_json, base_url, project_id, cloud_region, dataset_id, fhir_store_id
):
    """Deletes FHIR resources that match a search query."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    # The search query in this request deletes all Observations
    # with a status of 'cancelled'.
    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/Observation".format(
        url, dataset_id, fhir_store_id
    )
    # The search query is passed in as a query string parameter.
    params = {"status": "cancelled"}

    # Make an authenticated API request
    session = get_session(service_account_json)

    response = session.delete(resource_path, params=params)
    print(response.url)
    if response.status_code != 404:  # Don't consider missing to be error
        response.raise_for_status()

    print("Conditionally deleted all Observations with status='cancelled'.")

    return response


# [END healthcare_conditional_delete_resource]


# [START healthcare_patch_resource]
def patch_resource(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
    resource_id,
):
    """Updates part of an existing resource.."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, resource_type, resource_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/json-patch+json"}

    body = json.dumps([{"op": "replace", "path": "/active", "value": False}])

    response = session.patch(resource_path, headers=headers, data=body)
    response.raise_for_status()

    resource = response.json()

    print("Patched {} resource:".format(resource["resourceType"]))
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_patch_resource]


# [START healthcare_conditional_patch_resource]
def conditional_patch_resource(
    service_account_json, base_url, project_id, cloud_region, dataset_id, fhir_store_id
):
    """
    If a resource is found based on the search criteria specified in
    the query parameters, updates part of that resource by
    applying the operations specified in a JSON Patch document.
    """
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    # The search query in this request updates all Observations
    # if the subject of the Observation is a particular patient.
    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/Observation".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/json-patch+json"}

    body = json.dumps(
        [
            {
                "op": "replace",
                "path": "/valueQuantity/value",
                # Sets the BPM for all matching Observations to 80. This
                # is the portion of the request being patched.
                "value": 80,
            }
        ]
    )

    # The search query is passed in as a query string parameter.
    params = {"identifier": "my-code-system|ABC-12345"}

    response = session.patch(resource_path, headers=headers, params=params, data=body)
    response.raise_for_status()

    print(response.url)

    resource = response.json()

    print(
        "Conditionally patched all Observations with the "
        "identifier 'my-code-system|ABC-12345' to use a BPM of 80."
    )
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_conditional_patch_resource]


# [START healthcare_search_resources_get]
def search_resources_get(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_type,
):
    """
    Searches resources in the given FHIR store using the
    searchResources GET method.
    """
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}".format(
        url, dataset_id, fhir_store_id, resource_type
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    response = session.get(resource_path)
    response.raise_for_status()

    resources = response.json()

    print(
        "Using GET request, found a total of {} {} resources:".format(
            resources["total"], resource_type
        )
    )
    print(json.dumps(resources, indent=2))

    return resources


# [END healthcare_search_resources_get]


# [START healthcare_search_resources_post]
def search_resources_post(
    service_account_json, base_url, project_id, cloud_region, dataset_id, fhir_store_id
):
    """
    Searches resources in the given FHIR store using the
    _search POST method and a query string containing the
    information to search for. In this sample, the search criteria is
    'family:exact=Smith' on a Patient resource.
    """
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    fhir_store_path = "{}/datasets/{}/fhirStores/{}/fhir".format(
        url, dataset_id, fhir_store_id
    )

    resource_path = "{}/Patient/_search?family:exact=Smith".format(fhir_store_path)

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    response = session.post(resource_path, headers=headers)
    response.raise_for_status()

    resources = response.json()
    print(
        "Using POST request, found a total of {} Patient resources:".format(
            resources["total"]
        )
    )

    print(json.dumps(resources, indent=2))

    return resources


# [END healthcare_search_resources_post]


# [START healthcare_get_patient_everything]
def get_patient_everything(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    resource_id,
):
    """Gets all the resources in the patient compartment."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir/{}/{}".format(
        url, dataset_id, fhir_store_id, "Patient", resource_id
    )
    resource_path += "/$everything"

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    response = session.get(resource_path, headers=headers)
    response.raise_for_status()

    resource = response.json()

    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_get_patient_everything]


# [START healthcare_get_metadata]
def get_metadata(
    service_account_json, base_url, project_id, cloud_region, dataset_id, fhir_store_id
):
    """Gets the capabilities statement for a FHIR store."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    fhir_store_path = "{}/datasets/{}/fhirStores/{}/fhir/metadata".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    response = session.get(fhir_store_path)
    response.raise_for_status()

    metadata = response.json()

    print(json.dumps(metadata, indent=2))

    return metadata


# [END healthcare_get_metadata]


# [START healthcare_fhir_execute_bundle]
def execute_bundle(
    service_account_json,
    base_url,
    project_id,
    cloud_region,
    dataset_id,
    fhir_store_id,
    bundle,
):
    """Executes the operations in the given bundle."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    resource_path = "{}/datasets/{}/fhirStores/{}/fhir".format(
        url, dataset_id, fhir_store_id
    )

    # Make an authenticated API request
    session = get_session(service_account_json)

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    with open(bundle, "r") as bundle_file:
        bundle_file_content = bundle_file.read()

    response = session.post(resource_path, headers=headers, data=bundle_file_content)
    response.raise_for_status()

    resource = response.json()

    print("Executed bundle from file: {}".format(bundle))
    print(json.dumps(resource, indent=2))

    return resource


# [END healthcare_fhir_execute_bundle]


def parse_command_line_args():
    """Parses command line arguments."""

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--service_account_json",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Path to service account JSON file.",
    )

    parser.add_argument("--base_url", default=_BASE_URL, help="Healthcare API URL.")

    parser.add_argument(
        "--project_id",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        help="GCP project name",
    )

    parser.add_argument("--cloud_region", default="us-central1", help="GCP region")

    parser.add_argument("--dataset_id", default=None, help="Name of dataset")

    parser.add_argument("--fhir_store_id", default=None, help="Name of FHIR store")

    parser.add_argument(
        "--resource_type",
        default=None,
        help="The type of resource. First letter must be capitalized",
    )

    parser.add_argument(
        "--resource_id", default=None, help="Identifier for a FHIR resource"
    )

    parser.add_argument(
        "--patient_id",
        default=None,
        help="Identifier for a Patient resource. Can be used as a reference "
        "for an Encounter/Observation",
    )

    parser.add_argument(
        "--encounter_id",
        default=None,
        help="Identifier for an Encounter resource. Can be used as a "
        "reference for an Observation",
    )

    parser.add_argument(
        "--bundle",
        default=None,
        help="Name of file containing bundle of operations to execute",
    )

    parser.add_argument(
        "--uri_prefix", default=None, help="Prefix of gs:// URIs for import and export"
    )

    parser.add_argument("--version_id", default=None, help="Version of a FHIR resource")

    command = parser.add_subparsers(dest="command")

    command.add_parser("create-patient", help=create_patient.__doc__)
    command.add_parser("create-encounter", help=create_encounter.__doc__)
    command.add_parser("create-observation", help=create_observation.__doc__)
    command.add_parser("delete-resource", help=delete_resource.__doc__)
    command.add_parser(
        "conditional-delete-resource", help=conditional_delete_resource.__doc__
    )
    command.add_parser("get-resource", help=get_resource.__doc__)
    command.add_parser("list-resource-history", help=list_resource_history.__doc__)
    command.add_parser("execute-bundle", help=execute_bundle.__doc__)
    command.add_parser("get-resource-history", help=get_resource_history.__doc__)
    command.add_parser("delete-resource-purge", help=delete_resource_purge.__doc__)
    command.add_parser("update-resource", help=update_resource.__doc__)
    command.add_parser(
        "conditional-update-resource", help=conditional_update_resource.__doc__
    )
    command.add_parser("patch-resource", help=patch_resource.__doc__)
    command.add_parser(
        "conditional-patch-resource", help=conditional_patch_resource.__doc__
    )
    command.add_parser("search-resources-get", help=search_resources_get.__doc__)
    command.add_parser("search-resources-post", help=search_resources_get.__doc__)
    command.add_parser("get-patient-everything", help=get_patient_everything.__doc__)
    command.add_parser("get-metadata", help=get_metadata.__doc__)

    return parser.parse_args()


def run_command(args):
    """Calls the program using the specified command."""
    if args.project_id is None:
        print(
            "You must specify a project ID or set the "
            '"GOOGLE_CLOUD_PROJECT" environment variable.'
        )
        return

    elif args.command == "create-patient":
        create_patient(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
        )

    elif args.command == "create-encounter":
        create_encounter(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.patient_id,
        )

    elif args.command == "create-observation":
        create_observation(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.patient_id,
            args.encounter_id,
        )

    elif args.command == "delete-resource":
        delete_resource(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
            args.resource_id,
        )

    elif args.command == "conditional-delete-resource":
        conditional_delete_resource(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
        )

    elif args.command == "get-resource":
        get_resource(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
            args.resource_id,
        )

    elif args.command == "execute-bundle":
        execute_bundle(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.bundle,
        )

    elif args.command == "list-resource-history":
        list_resource_history(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
            args.resource_id,
        )

    elif args.command == "get-resource-history":
        get_resource_history(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
            args.resource_id,
            args.version_id,
        )

    elif args.command == "delete-resource-purge":
        delete_resource_purge(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
            args.resource_id,
        )

    elif args.command == "update-resource":
        update_resource(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
            args.resource_id,
        )

    elif args.command == "conditional-update-resource":
        conditional_update_resource(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.patient_id,
            args.encounter_id,
        )

    elif args.command == "patch-resource":
        patch_resource(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
            args.resource_id,
        )

    elif args.command == "conditional-patch-resource":
        conditional_patch_resource(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
        )

    elif args.command == "search-resources-get":
        search_resources_get(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_type,
        )

    elif args.command == "search-resources-post":
        search_resources_post(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
        )

    elif args.command == "get-patient-everything":
        get_patient_everything(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
            args.resource_id,
        )

    elif args.command == "get-metadata":
        get_metadata(
            args.service_account_json,
            args.base_url,
            args.project_id,
            args.cloud_region,
            args.dataset_id,
            args.fhir_store_id,
        )


def main():
    args = parse_command_line_args()
    run_command(args)


if __name__ == "__main__":
    main()
