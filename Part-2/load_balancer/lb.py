import asyncio
import random
from quart import Quart, jsonify, request
import aiohttp
import asyncpg
from manager import Manager
from hash_ring import HashRing
from colorama import Fore, Style

app = Quart(__name__)
manager = Manager(on_server_dead=None)  # We'll override callback later

# -------------------- DB --------------------
LB_DB_POOL = None

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_NAME = "studdb"
DB_HOST = "postgres"  # container name of postgres in docker-compose
DB_PORT = 5432

# -------------------- In-memory metadata --------------------
# Per-shard asyncio locks for cooperative multitasking
shard_locks = {}

# -------------------- Helpers --------------------
def find_shard_for_id(stud_id, ShardT):
    for s in ShardT:
        low = s["stud_id_low"]
        size = s["shard_size"]
        if low <= stud_id < (low + size):
            return s["shard_id"]
    return None

def shards_for_range(low_id, high_id, ShardT):
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

async def call_server_read(server, payload, timeout=5):
    url = f"http://{server}:5000/read"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            return resp.status, data

async def call_server_copy(server, payload, timeout=10):
    url = f"http://{server}:5000/copy"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            return resp.status, data

# -------------------- Lifecycle --------------------
@app.before_serving
async def startup():
    global LB_DB_POOL
    LB_DB_POOL = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )
    await manager.start()
    manager.on_server_dead = handle_server_failure

@app.after_serving
async def shutdown():
    await manager.stop()
    await LB_DB_POOL.close()

# -------------------- Server Failure --------------------
async def handle_server_failure(dead_server):
    print(f"[Recover] Handling failure of {dead_server}")

    # 1. Find affected shards
    async with LB_DB_POOL.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch("SELECT shard_id, servers FROM ShardT")
            affected_shards = [r['shard_id'] for r in rows if dead_server in r['servers']]

    # 2. Remove server
    await manager.remove_server(dead_server)

    # 3. Spawn replacement server
    new_name = f"ServerAuto{manager.counter}"
    manager.counter += 1
    await manager.spawn_server(new_name)

    # 4. Wait heartbeat
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

    # 5. Configure new server for affected shards
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"http://{new_name}:5000/config",
                json={"shards": affected_shards}
            ) as resp:
                if resp.status == 200:
                    print(f"[Recover] Configured {new_name} with {affected_shards}")
        except Exception as e:
            print(f"[Recover] Error configuring {new_name}: {e}")

    # 6. Restore data from healthy replica
    for shard_id in affected_shards:
        async with LB_DB_POOL.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT servers FROM ShardT WHERE shard_id=$1", shard_id)
                healthy_hosts = [h for h in row['servers'] if h != dead_server]
        if healthy_hosts:
            donor = healthy_hosts[0]
            status, data = await call_server_copy(donor, payload={"shards":[shard_id]})
            if status == 200 and shard_id in data:
                rows = data[shard_id]
                if rows:
                    payload = {
                        "shard": shard_id,
                        "valid_at": 0,
                        "data": [{"stud_id": r["stud_id"], "stud_name": r["stud_name"], "stud_marks": r["stud_marks"]} for r in rows],
                        "admin": True
                    }
                    await call_server_write(new_name, payload)

# -------------------- Endpoints --------------------
@app.route("/init", methods=["POST"])
async def init():
    payload = await request.get_json()
    shards = payload.get("shards", [])
    servers = payload.get("servers", {})

    if not shards or not servers:
        return jsonify({"error": "shards and servers required"}), 400

    # ✅ Invert mapping from server->shards to shard->servers
    shard_to_servers = {}
    for server_name, shard_list in servers.items():
        for sid in shard_list:
            shard_to_servers.setdefault(sid, []).append(server_name)

    async with LB_DB_POOL.acquire() as conn:
        async with conn.transaction():
            # Create ShardT table if not exists
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS ShardT (
                shard_id TEXT PRIMARY KEY,
                stud_id_low INTEGER,
                shard_size INTEGER,
                valid_at INTEGER,
                servers TEXT[]
            )
            """)

            # Clear existing entries
            await conn.execute("DELETE FROM ShardT")

            # Insert new shard entries (✅ using inverted map)
            for s in shards:
                sid = s["shard_id"]
                low = s["stud_id_low"]
                size = s["shard_size"]
                shard_servers = shard_to_servers.get(sid, [])
                await conn.execute(
                    "INSERT INTO ShardT(shard_id, stud_id_low, shard_size, valid_at, servers) VALUES($1,$2,$3,$4,$5)",
                    sid, low, size, 0, shard_servers
                )

    # Spawn and configure servers
    for server_name, shard_list in servers.items():
        if server_name not in manager.replicas:
            await manager.spawn_server(server_name)

        # Wait for heartbeat
        async def wait_hb(server_name, retries=10, delay=2):
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

        ready = await wait_hb(server_name)
        if not ready:
            print(f"Warning: {server_name} did not respond to heartbeat")
            continue

        # Configure server with its shards
        async with aiohttp.ClientSession() as session:
            try:
                await session.post(f"http://{server_name}:5000/config", json={"shards": shard_list})
            except Exception as e:
                print(f"Failed to configure {server_name}: {e}")

    # Initialize per-shard locks
    for s in shards:
        shard_locks[s["shard_id"]] = asyncio.Lock()

    return jsonify({"status": "success", "shards": shards, "servers": servers}), 200


@app.route("/status", methods=["GET"])
async def status():
    async with LB_DB_POOL.acquire() as conn:
        shards = await conn.fetch("SELECT * FROM ShardT")
        return jsonify({
            "ShardT": [dict(s) for s in shards],
            "replicas": list(manager.replicas)
        }), 200

@app.route("/write", methods=["POST"])
async def lb_write():
    payload = await request.get_json()
    rows = payload.get("data", [])
    if not rows:
        return jsonify({"error": "no rows provided"}), 400

    results = {}
    async with LB_DB_POOL.acquire() as conn:
        for row in rows:
            shard_id = row["shard_id"]
            async with conn.transaction():
                shard_row = await conn.fetchrow("SELECT valid_at, servers FROM ShardT WHERE shard_id=$1 FOR UPDATE", shard_id)
                vat = shard_row['valid_at']
                servers = shard_row['servers']
                new_vat = vat + 1

                # Forward to all replicas
                server_req = {"shard": shard_id, "valid_at": new_vat, "data":[row]}
                failures = []
                for host in servers:
                    status, _ = await call_server_write(host, server_req)
                    if status != 200:
                        failures.append(host)

                # Update LB ShardT valid_at
                await conn.execute("UPDATE ShardT SET valid_at=$1 WHERE shard_id=$2", new_vat, shard_id)
                results[shard_id] = {"inserted":1, "failures":failures}

    return jsonify({"status": "completed", "details": results}), 200

@app.route("/read", methods=["POST"])
async def lb_read():
    payload = await request.get_json()
    stud_range = payload.get("Stud_id", {})
    low = stud_range.get("low")
    high = stud_range.get("high")
    if low is None or high is None:
        return jsonify({"error": "low/high required"}), 400

    async with LB_DB_POOL.acquire() as conn:
        shards_rows = await conn.fetch("SELECT * FROM ShardT")
        ShardT = [dict(s) for s in shards_rows]
    shard_ids = shards_for_range(int(low), int(high), ShardT)
    results = []

    async with aiohttp.ClientSession() as session:
        for shard_id in shard_ids:
            shard_row = next(s for s in ShardT if s["shard_id"]==shard_id)
            servers = shard_row["servers"]
            host = random.choice(servers)
            req = {"shard": shard_id, "stud_id":{"low":low,"high":high}, "valid_at": shard_row["valid_at"]}
            try:
                async with session.post(f"http://{host}:5000/read", json=req) as resp:
                    if resp.status==200:
                        data = await resp.json()
                        results.extend(data.get("data", []))
            except:
                pass

    return jsonify({"shards_queried": shard_ids, "data": results, "status":"success"}), 200

@app.route("/update", methods=["PUT"])
async def lb_update():
    payload = await request.get_json()
    row = payload.get("data")
    stud_id = row.get("stud_id")
    shard_id = row.get("shard_id")
    if not stud_id or not shard_id:
        return jsonify({"error": "stud_id and shard_id required"}), 400

    async with LB_DB_POOL.acquire() as conn:
        async with conn.transaction():
            shard_row = await conn.fetchrow(
                "SELECT valid_at, servers FROM ShardT WHERE shard_id=$1 FOR UPDATE",
                shard_id
            )
            vat = shard_row['valid_at']
            servers = shard_row['servers']
            new_vat = vat + 1

            # Forward to all replicas
            server_req = {"shard": shard_id, "valid_at": new_vat, "data":[row]}
            failures = []
            for host in servers:
                status, _ = await call_server_write(host, server_req)
                if status != 200:
                    failures.append(host)

            # Update LB valid_at
            await conn.execute("UPDATE ShardT SET valid_at=$1 WHERE shard_id=$2", new_vat, shard_id)

    return jsonify({
        "status": "completed",
        "failures": failures,
        "valid_at": new_vat
    }), 200

@app.route("/del", methods=["DELETE"])
async def lb_delete():
    payload = await request.get_json()
    stud_id = payload.get("stud_id")
    shard_id = payload.get("shard_id")
    if not stud_id or not shard_id:
        return jsonify({"error": "stud_id and shard_id required"}), 400

    async with LB_DB_POOL.acquire() as conn:
        async with conn.transaction():
            shard_row = await conn.fetchrow(
                "SELECT valid_at, servers FROM ShardT WHERE shard_id=$1 FOR UPDATE",
                shard_id
            )
            vat = shard_row['valid_at']
            servers = shard_row['servers']
            new_vat = vat + 1

            # Forward delete to all replicas
            server_req = {"shard": shard_id, "valid_at": new_vat, "stud_id": stud_id}
            failures = []
            for host in servers:
                try:
                    status, _ = await call_server_write(host, server_req)
                    if status != 200:
                        failures.append(host)
                except:
                    failures.append(host)

            # Update LB valid_at
            await conn.execute("UPDATE ShardT SET valid_at=$1 WHERE shard_id=$2", new_vat, shard_id)

    return jsonify({
        "status": "completed",
        "failures": failures,
        "valid_at": new_vat
    }), 200

# -------------------- Run --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, use_reloader=False)
