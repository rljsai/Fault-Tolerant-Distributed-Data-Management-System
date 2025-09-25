import random
from quart import Quart, jsonify, request
import aiohttp
import asyncio
from manager import Manager

async def handle_server_failure(dead_server):
    print(f"[Recover] Handling failure of {dead_server}")

    # 1. Figure out shards *before* removing the server
    affected_shards = [sid for sid, hosts in MapT.items() if dead_server in hosts]

    # 2. Remove dead server cleanly
    try:
        await manager.remove_server(dead_server)
    except Exception as e:
        print(f"[Recover] Failed to remove {dead_server}: {e}")

    # Clean MapT (remove dead server from all shard mappings)
    for sid, hosts in list(MapT.items()):
        MapT[sid] = [h for h in hosts if h in manager.replicas]

    # If no shards were on this server → nothing to do
    if not affected_shards:
        print(f"[Recover] No shards were mapped to {dead_server}")
        return

    # 3. Spawn replacement
    new_name = f"ServerAuto{manager.counter}"
    manager.counter += 1
    await manager.spawn_server(new_name)

    # 4. Wait until heartbeat responds
    async def wait_for_heartbeat(server_name, retries=10, delay=2):
        async with aiohttp.ClientSession() as session:
            for _ in range(retries):
                try:
                    async with session.get(f"http://{server_name}:5000/heartbeat") as resp:
                        if resp.status == 200:
                            return True
                except:
                    pass
                await asyncio.sleep(delay)
        return False

    ready = await wait_for_heartbeat(new_name)
    if not ready:
        print(f"[Recover] {new_name} never responded to heartbeat, aborting recovery")
        return

    # 5. Configure new server with affected shards
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"http://{new_name}:5000/config", json={"shard_ids": affected_shards}
            ) as resp:
                if resp.status == 200:
                    print(f"[Recover] Configured {new_name} with {affected_shards}")
                else:
                    print(f"[Recover] Failed to configure {new_name}, status={resp.status}")
        except Exception as e:
            print(f"[Recover] Error configuring {new_name}: {e}")

    # 6. Restore shard data from healthy replicas
    for shard_id in affected_shards:
        healthy_hosts = [h for h in MapT[shard_id] if h in manager.replicas and h != dead_server]
        if not healthy_hosts:
            print(f"[Recover] No healthy replicas available for {shard_id}, cannot restore data")
            continue

        donor = healthy_hosts[0]
        try:
            status, data = await call_server_copy(donor, params={"shard_id": shard_id})
            if status == 200 and "rows" in data and isinstance(data["rows"], list):
                rows = data["rows"]
                if rows:
                    # ✅ Reshape rows into proper /write format
                    payload = {
                        "shard": shard_id,
                        "curr_idx": 0,
                        "data": [
                            {
                                "Stud_id": r["stud_id"],
                                "Stud_name": r["stud_name"],
                                "Stud_marks": r["stud_marks"]
                            }
                            for r in rows
                        ]
                    }
                    async with aiohttp.ClientSession() as session:
                        await session.post(
                            f"http://{new_name}:5000/write",
                            json=payload
                        )
                    print(f"[Recover] Restored {len(rows)} rows of {shard_id} to {new_name}")
                else:
                    print(f"[Recover] No rows to restore for {shard_id}")
            else:
                print(f"[Recover] Copy from {donor} failed or returned bad data: {data}")
        except Exception as e:
            print(f"[Recover] Exception while copying shard {shard_id} from {donor}: {e}")

        # Update MapT with new server
        MapT[shard_id].append(new_name)





app = Quart(__name__)
manager = Manager(on_server_dead=handle_server_failure)

# --- In-memory metadata ---
# ShardT: list of shards with { shard_id, stud_id_low, shard_size, valid_idx }
ShardT = []    # populated by /init
# MapT: dict shard_id -> [server_hostnames]
MapT = {}
# per-shard locks
shard_locks = {}


# ---------- lifecycle ----------

@app.before_serving
async def startup():
    await manager.start()

@app.after_serving
async def shutdown():
    await manager.stop()

# ---------- helpers ----------

def find_shard_for_id(stud_id: int):
    for s in ShardT:
        low = s["stud_id_low"]
        size = s["shard_size"]
        if low <= stud_id < (low + size):
            return s["shard_id"]
    return None

def shards_for_range(low_id: int, high_id: int):
    res = []
    for s in ShardT:
        low = s["stud_id_low"]
        size = s["shard_size"]
        high = low + size - 1
        if not (high < low_id or low > high_id):
            res.append(s["shard_id"])
    return res

async def call_server_write(server, payload, timeout=5):
    url = f"http://{server}:5000/write"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            return resp.status, data

async def call_server_read(server, params, timeout=5):
    url = f"http://{server}:5000/read"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            return resp.status, data

async def call_server_copy(server, params, timeout=10):
    url = f"http://{server}:5000/copy"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            return resp.status, data


# ---------- endpoints ----------

@app.route("/status", methods=["GET"])
async def status():
    return jsonify({
        "ShardT": ShardT,
        "MapT": MapT,
        "replicas": list(manager.replicas)
    }), 200


@app.route("/init", methods=["POST"])
async def init():
    """
    Payload should be:
    {
      "shards": [{"stud_id_low":0,"shard_id":"sh1","shard_size":4096}, ...],
      "servers": {"Server0":["sh1","sh2"], "Server1":["sh3"], ...}
    }
    """
    payload = await request.get_json()
    shards = payload.get("shards", [])
    servers = payload.get("servers", {})

    # ---------------- ShardT ----------------
    ShardT.clear()
    for s in shards:
        entry = {
            "shard_id": s["shard_id"],
            "stud_id_low": int(s["stud_id_low"]),
            "shard_size": int(s["shard_size"]),
            "valid_idx": 0
        }
        ShardT.append(entry)

    # ---------------- MapT ----------------
    MapT.clear()
    for sid in [s["shard_id"] for s in shards]:
        MapT[sid] = []

    # ---------------- Spawn servers + config ----------------
    for server_name, shard_list in servers.items():
        # spawn if not present
        if server_name not in manager.replicas:
            await manager.spawn_server(server_name)

        # wait for heartbeat before sending config
        ready = False
        for _ in range(10):  # retry up to 10s
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://{server_name}:5000/heartbeat") as resp:
                        if resp.status == 200:
                            ready = True
                            break
            except:
                pass
            await asyncio.sleep(1)

        if not ready:
            print(f"Warning: {server_name} did not respond to heartbeat in time")
            continue

        # update MapT
        MapT.update({k: (MapT.get(k, []) + [server_name]) for k in shard_list})

        # call /config on the server
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"http://{server_name}:5000/config",
                    json={"shard_ids": shard_list}
                ) as resp:
                    await resp.text()  # consume body
                    if resp.status == 200:
                        print(f"[Init] Configured {server_name} with {shard_list}")
                    else:
                        print(f"[Init] /config failed on {server_name}, status={resp.status}")
            except Exception as e:
                print(f"Warning: /config call to {server_name} failed: {e}")

    # ---------------- Locks ----------------
    shard_locks.clear()
    for s in ShardT:
        shard_locks[s["shard_id"]] = asyncio.Lock()

    return jsonify({
        "message": "initialized",
        "ShardT": ShardT,
        "MapT": MapT
    }), 200


@app.route("/add", methods=["POST"])
async def add_replicas():
    body = await request.get_json()
    n = body.get("n", 0)
    hostnames = body.get("hostnames", [])
    if not isinstance(hostnames, list):
        return jsonify({"message": "Invalid hostnames"}, 400)

    # Spawn provided hostnames; spawn random if not enough
    to_spawn = n - len(hostnames)
    for h in hostnames:
        if h not in manager.replicas:
            await manager.spawn_server(h)

    for _ in range(to_spawn):
        new_name = f"ServerAuto{manager.counter}"
        manager.counter += 1
        await manager.spawn_server(new_name)

    return jsonify({"message": manager.list_servers(), "status": "successful"}), 200



@app.route("/rm", methods=["DELETE"])
async def rm_replicas():
    body = await request.get_json()
    n = body.get("n", 0)
    hostnames = body.get("hostnames", [])
    if not isinstance(hostnames, list):
        return jsonify({"message": "Invalid hostnames"}, 400)

    # remove specified
    for h in hostnames:
        if h in manager.replicas:
            await manager.remove_server(h)

    # if n > len(hostnames) remove more random
    remaining = n - len(hostnames)
    if remaining > 0:
        current = list(manager.replicas)
        for i in range(min(remaining, len(current))):
            await manager.remove_server(current[i])

    # update MapT entries to remove missing hosts
    for sid, hosts in list(MapT.items()):
        MapT[sid] = [h for h in hosts if h in manager.replicas]

    return jsonify({"message": manager.list_servers(), "status": "successful"}), 200


# -------------------- Write --------------------
@app.route("/write", methods=["POST"])
async def lb_write():
    """
    Client request: { "data": [ {Stud_id, Stud_name, Stud_marks}, ... ] }
    LB figures out shard, then forwards to servers in spec format.
    """
    payload = await request.get_json()
    rows = payload.get("data", [])
    if not rows:
        return jsonify({"error": "no rows provided"}), 400

    results = {}
    for row in rows:
        shard_id = find_shard_for_id(int(row["Stud_id"]))
        if not shard_id:
            return jsonify({"error": f"Stud_id {row['Stud_id']} not in any shard"}), 400

        hosts = MapT.get(shard_id, [])
        if not hosts:
            return jsonify({"error": f"No replicas for shard {shard_id}"}), 500

        # prepare request for server spec
        server_req = {
            "shard": shard_id,
            "curr_idx": next(s["valid_idx"] for s in ShardT if s["shard_id"] == shard_id),
            "data": [row]
        }

        failures = []
        for host in hosts:
            try:
                async with aiohttp.ClientSession() as session:
                    resp = await session.post(f"http://{host}:5000/write", json=server_req)
                    if resp.status != 200:
                        failures.append(host)
            except Exception as e:
                print(f"[LB write] failed to {host}: {e}")
                failures.append(host)

        # update index
        for s in ShardT:
            if s["shard_id"] == shard_id:
                s["valid_idx"] += 1

        results[shard_id] = {"inserted": 1, "failures": failures}

    return jsonify({"status": "completed", "details": results}), 200


# -------------------- Read --------------------
@app.route("/read", methods=["POST"])
async def lb_read():
    """
    Client request: { "Stud_id": {"low": <int>, "high": <int>} }
    LB maps range → shards → queries replicas.
    """
    payload = await request.get_json()
    stud_range = payload.get("Stud_id", {})
    low = stud_range.get("low")
    high = stud_range.get("high")

    if low is None or high is None:
        return jsonify({"error": "low/high required"}), 400

    shard_ids = shards_for_range(int(low), int(high))
    if not shard_ids:
        return jsonify({"data": [], "shards_queried": [], "status": "success"}), 200

    results = []
    async with aiohttp.ClientSession() as session:
        for shard_id in shard_ids:
            hosts = MapT.get(shard_id, [])
            if not hosts:
                continue

            req = {"shard": shard_id, "Stud_id": {"low": low, "high": high}}
            host = random.choice(hosts)

            try:
                async with session.post(f"http://{host}:5000/read", json=req) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results.extend(data)
            except Exception as e:
                print(f"[LB read] failed to {host}: {e}")

    return jsonify({
        "shards_queried": shard_ids,
        "data": results,
        "status": "success"
    }), 200

# -------------------- Update --------------------
@app.route("/update", methods=["PUT"])
async def lb_update():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    if not stud_id:
        return jsonify({"error": "Stud_id required"}), 400

    shard_id = find_shard_for_id(int(stud_id))
    if shard_id is None:
        return jsonify({"error": "Stud_id out of range"}), 400

    # enrich payload with Shard_id
    payload["data"]["Shard_id"] = shard_id

    lock = shard_locks.get(shard_id)
    async with lock:
        hosts = MapT.get(shard_id, [])
        failures = []
        async with aiohttp.ClientSession() as session:
            for host in hosts:
                try:
                    async with session.put(f"http://{host}:5000/update", json=payload) as resp:
                        if resp.status != 200:
                            failures.append(host)
                except Exception:
                    failures.append(host)

        if failures:
            return jsonify({"status": "partial", "failed": failures}), 500

    # update valid_idx metadata (optional but consistent with write)
    for s in ShardT:
        if s["shard_id"] == shard_id:
            s["valid_idx"] += 1
            break

    return jsonify({
        "message": f"Data entry for Stud_id:{stud_id} updated",
        "status": "success"
    }), 200


# -------------------- Delete --------------------
@app.route("/del", methods=["DELETE"])
async def lb_delete():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    if not stud_id:
        return jsonify({"error": "Stud_id required"}), 400

    shard_id = find_shard_for_id(int(stud_id))
    if shard_id is None:
        return jsonify({"error": "Stud_id out of range"}), 400

    # enrich payload with Shard_id
    payload["Shard_id"] = shard_id

    lock = shard_locks.get(shard_id)
    async with lock:
        hosts = MapT.get(shard_id, [])
        failures = []
        async with aiohttp.ClientSession() as session:
            for host in hosts:
                try:
                    async with session.delete(f"http://{host}:5000/del", json=payload) as resp:
                        if resp.status != 200:
                            failures.append(host)
                except Exception:
                    failures.append(host)

        if failures:
            return jsonify({"status": "partial", "failed": failures}), 500

    return jsonify({
        "message": f"Data entry with Stud_id:{stud_id} removed",
        "status": "success"
    }), 200


# Run
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)