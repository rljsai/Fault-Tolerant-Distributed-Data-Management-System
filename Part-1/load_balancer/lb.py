import random
from quart import Quart, jsonify, request
import aiohttp
from manager import Manager

app = Quart(__name__)
manager = Manager()

@app.before_serving
async def startup():
    await manager.start()

@app.after_serving
async def shutdown():
    await manager.stop()

    
@app.route("/rep", methods=["GET"])
async def list_replicas():
    data = manager.list_servers()
    return jsonify({"message": data, "status": "successful"}), 200


@app.route("/add", methods=["POST"])
async def add_replicas():
    body = await request.get_json()
    n = body.get("n")
    hostnames = body.get("hostnames", [])

    if n is None or not isinstance(hostnames, list):
        return jsonify({"message": "Invalid input", "status": "error"}), 400
    if len(hostnames) > n:
        return jsonify({"message": "Too many hostnames", "status": "error"}), 400

    for h in hostnames:
        await manager.spawn_server(h)

    data = manager.list_servers()
    return jsonify({"message": data, "status": "successful"}), 200


@app.route("/rm", methods=["DELETE"])
async def remove_replicas():
    body = await request.get_json()
    n = body.get("n")
    hostnames = body.get("hostnames", [])

    if n is None or not isinstance(hostnames, list):
        return jsonify({"message": "Invalid input", "status": "error"}), 400
    if len(hostnames) > n:
        return jsonify({"message": "Too many hostnames", "status": "error"}), 400

    for h in hostnames:
        await manager.remove_server(h)

    data = manager.list_servers()
    return jsonify({"message": data, "status": "successful"}), 200


@app.route("/<path:subpath>", methods=["GET"])
async def forward_request(subpath):
    rid = request.args.get("rid")
    if rid is None:
        rid = random.randint(1, 1_000_000)
    else:
        rid = int(rid)

    tried = set()
    max_retries = len(manager.replicas)  # retry at most once per server
    server = manager.get_server_for_request(rid)

    for _ in range(max_retries):
        if not server or server in tried:
            break
        tried.add(server)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://{server}:5000/{subpath}") as resp:
                    data = await resp.json()
                    return jsonify(data), resp.status
        except Exception as e:
            print(f"[Retry] Server {server} failed for rid={rid}: {e}")
            # try the next clockwise server
            server = manager.ring.get_next_server(server)

    return jsonify({
        "message": "All retries failed, no servers available",
        "status": "error"
    }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
