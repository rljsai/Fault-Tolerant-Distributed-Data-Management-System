from flask import Flask, jsonify, request
import requests
from manager import Manager
import random

app = Flask(__name__)
manager = Manager()


@app.route("/rep", methods=["GET"])
def list_replicas():
    data = manager.list_servers()
    return jsonify({"message": data, "status": "successful"}), 200


@app.route("/add", methods=["POST"])
def add_replicas():
    body = request.get_json()
    n = body.get("n")
    hostnames = body.get("hostnames", [])

    if n is None or not isinstance(hostnames, list):
        return jsonify({"message": "Invalid input", "status": "error"}), 400
    if len(hostnames) > n:
        return jsonify({"message": "Too many hostnames", "status": "error"}), 400

    manager.add_servers(hostnames)
    data = manager.list_servers()
    return jsonify({"message": data, "status": "successful"}), 200


@app.route("/rm", methods=["DELETE"])
def remove_replicas():
    body = request.get_json()
    n = body.get("n")
    hostnames = body.get("hostnames", [])

    if n is None or not isinstance(hostnames, list):
        return jsonify({"message": "Invalid input", "status": "error"}), 400
    if len(hostnames) > n:
        return jsonify({"message": "Too many hostnames", "status": "error"}), 400

    manager.remove_servers(hostnames)
    data = manager.list_servers()
    return jsonify({"message": data, "status": "successful"}), 200


# @app.route("/<path:subpath>", methods=["GET"])
# def forward_request(subpath):
#     request_id = random.randint(1, 1_000_000)
#     server = manager.get_server_for_request( request_id)
#     if not server:
#         return jsonify({"message": "No servers available", "status": "error"}), 500

#     try:
#         # 🚀 Use Docker network alias instead of localhost:port
#         url = f"http://{server}:5000/{subpath}"
#         resp = requests.get(url)
#         return jsonify(resp.json()), resp.status_code
#     except Exception as e:
#         return jsonify({"message": f"Error forwarding to {server}: {str(e)}", "status": "error"}), 500

@app.route("/<path:subpath>", methods=["GET"])
def forward_request(subpath):
    rid = request.args.get("rid")
    if rid is None:
        rid = random.randint(1, 1_000_000)
    else:
        rid = int(rid)

    server = manager.get_server_for_request(rid)
    if not server:
        return jsonify({"message": "No servers available", "status": "error"}), 500

    try:
        url = f"http://{server}:5000/{subpath}"
        resp = requests.get(url)
        return jsonify(resp.json()), resp.status_code
    except Exception as e:
        return jsonify({
            "message": f"Error forwarding to {server}: {str(e)}",
            "status": "error"
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
