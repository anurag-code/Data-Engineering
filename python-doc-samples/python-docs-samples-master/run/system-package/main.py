# Copyright 2019 Google, LLC.
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

from flask import Flask, make_response, request
import os
import subprocess
import sys


app = Flask(__name__)


# [START run_system_package_handler]
@app.route("/diagram.png", methods=["GET"])
def index():
    # Takes an HTTP GET request with query param dot and
    # returns a png with the rendered DOT diagram in a HTTP response.
    try:
        image = create_diagram(request.args.get("dot"))
        response = make_response(image)
        response.headers.set("Content-Type", "image/png")
        return response

    except Exception as e:
        print("error: {}".format(e))

        # Flush the stdout to avoid log buffering.
        sys.stdout.flush()

        # If no graphviz definition or bad graphviz def, return 400
        if "syntax" in str(e):
            return "Bad Request: {}".format(e), 400

        return "Internal Server Error", 500


# [END run_system_package_handler]


# [START run_system_package_exec]
def create_diagram(dot):
    # Generates a diagram based on a graphviz DOT diagram description.
    if not dot:
        raise Exception("syntax: no graphviz definition provided")

    dot_args = [  # These args add a watermark to the dot graphic.
        "-Glabel=Made on Cloud Run",
        "-Gfontsize=10",
        "-Glabeljust=right",
        "-Glabelloc=bottom",
        "-Gfontcolor=gray",
        "-Tpng",
    ]

    # Uses local `dot` binary from Graphviz:
    # https://graphviz.gitlab.io
    image = subprocess.run(
        ["dot"] + dot_args, input=dot.encode("utf-8"), stdout=subprocess.PIPE
    ).stdout

    if not image:
        raise Exception("syntax: bad graphviz definition provided")
    return image


# [END run_system_package_exec]


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)
