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

# [START functions_pubsub_setup]
import base64
from google.cloud import pubsub_v1
import json
import os


# Instantiates a Pub/Sub client
publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.getenv('GCP_PROJECT')
# [END functions_pubsub_setup]


# [START functions_pubsub_publish]
# Publishes a message to a Cloud Pub/Sub topic.
def publish(request):
    request_json = request.get_json(silent=True)

    topic_name = request_json.get("topic")
    message = request_json.get("message")

    if not topic_name or not message:
        return ('Missing "topic" and/or "subscription" parameter.', 500)

    print(f'Publishing message to topic {topic_name}')

    # References an existing topic
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    message_json = json.dumps({
        'data': {'message': message},
    })
    message_bytes = message_json.encode('utf-8')
    print(message_bytes)

    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)

# [END functions_pubsub_publish]


# [START functions_pubsub_subscribe]
# Triggered from a message on a Cloud Pub/Sub topic.
def subscribe(event, context):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(event['data']))
# [END functions_pubsub_subscribe]
