from quart import Quart, jsonify, request
import os
import asyncpg


app = Quart(__name__)

SERVER_ID = os.environ.get("SERVER_ID", "default-server")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.environ.get("POSTGRES_DB", "studdb")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 5432))


# Global DB pool
db_pool: asyncpg.Pool = None

# Keep shard_ids assigned to this server
owned_shards = set()


@app.before_serving
async def startup():
    global db_pool
    db_pool = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS StudT (
                Stud_id INT PRIMARY KEY,
                Stud_name TEXT,
                Stud_marks INT,
                Shard_id TEXT
            );
        """)
    print(f"[{SERVER_ID}] Database initialized successfully")
@app.after_serving
async def shutdown():
    await db_pool.close()


# -------------------- Basic endpoints --------------------

@app.route("/home", methods=["GET"])
async def home():
    return jsonify({
        "message": f"Hello from server: {SERVER_ID}",
        "status": "successful"
    }), 200


@app.route("/heartbeat", methods=["GET"])
async def heartbeat():
    return jsonify({
        "message": "active",
        "status": "successful"
    }), 200

# -------------------- Config --------------------

@app.route("/config", methods=["POST"])
async def config():
    """
    Assign shards to this server.
    Request: { "shard_ids": [1,2,...] }
    """
    data = await request.get_json()
    shard_ids = data.get("shard_ids", [])
    owned_shards.update(shard_ids)

    return jsonify({
        "message": f"Configured shards {shard_ids} on {SERVER_ID}",
        "status": "successful"
    }), 200


# -------------------- Write --------------------
@app.route("/write", methods=["POST"])
async def write():
    """
    Request: { "shard": "shX", "curr_idx": <int>, "data": [ {Stud_id, Stud_name, Stud_marks}, ... ] }
    """
    data = await request.get_json()
    shard_id = data.get("shard")
    curr_idx = data.get("curr_idx")
    rows = data.get("data", [])

    if shard_id not in owned_shards:
        return jsonify({"error": f"This server does not own shard {shard_id}"}), 400

    async with db_pool.acquire() as conn:
        for row in rows:
            await conn.execute("""
                INSERT INTO StudT (Stud_id, Stud_name, Stud_marks, Shard_id)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (Stud_id)
                DO UPDATE SET Stud_name = EXCLUDED.Stud_name,
                              Stud_marks = EXCLUDED.Stud_marks,
                              Shard_id = EXCLUDED.Shard_id;
            """, row["Stud_id"], row["Stud_name"], row["Stud_marks"], shard_id)

    return jsonify({"status": "success", "inserted": len(rows)}), 200


# -------------------- Read --------------------
@app.route("/read", methods=["POST"])
async def read():
    """
    Request: { "shard": "shX", "Stud_id": {"low": <int>, "high": <int>} }
    """
    data = await request.get_json()
    shard_id = data.get("shard")
    stud_range = data.get("Stud_id", {})
    low = stud_range.get("low")
    high = stud_range.get("high")

    if shard_id not in owned_shards:
        return jsonify({"error": f"This server does not own shard {shard_id}"}), 400

    async with db_pool.acquire() as conn:
        if low is not None and high is not None:
            rows = await conn.fetch(
                "SELECT * FROM StudT WHERE Shard_id=$1 AND Stud_id BETWEEN $2 AND $3",
                shard_id, int(low), int(high)
            )
        else:
            rows = await conn.fetch("SELECT * FROM StudT WHERE Shard_id=$1", shard_id)

    return jsonify([dict(r) for r in rows]), 200


# -------------------- Copy --------------------

@app.route("/copy", methods=["GET"])
async def copy_shard():
    """
    Copy all rows from a shard (used for recovery).
    Query param: shard_id=<id>
    """
    shard_id = int(request.args.get("shard_id"))

    if shard_id not in owned_shards:
        return jsonify({"error": "This server does not own the shard"}), 400

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM StudT WHERE Shard_id=$1", shard_id)

    return jsonify([dict(r) for r in rows]), 200


# -------------------- Update --------------------
@app.route("/update", methods=["PUT"])
async def update():
    """
    Update marks/name for a student.
    Request: { "Stud_id": 2255, "data": {"Stud_id":2255,"Stud_name":"GHI","Stud_marks":30} }
    """
    data = await request.get_json()
    stud_id = data.get("Stud_id")
    update_data = data.get("data", {})

    if not stud_id or not update_data:
        return jsonify({"error": "Stud_id and data required"}), 400

    shard_id = update_data.get("Shard_id")
    if shard_id not in owned_shards:
        return jsonify({"error": f"This server does not own the shard {shard_id}"}), 400

    async with db_pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE StudT
            SET Stud_name=$1, Stud_marks=$2
            WHERE Stud_id=$3 AND Shard_id=$4
        """, update_data["Stud_name"], update_data["Stud_marks"], stud_id, shard_id)

    return jsonify({
        "message": f"Data entry for Stud_id:{stud_id} updated",
        "status": "success"
    }), 200


# -------------------- Delete --------------------
@app.route("/del", methods=["DELETE"])
async def delete():
    """
    Delete a student row.
    Request: { "Stud_id": 2255, "Shard_id": "sh1" }
    """
    data = await request.get_json()
    stud_id = data.get("Stud_id")
    shard_id = data.get("Shard_id")

    if not stud_id:
        return jsonify({"error": "Stud_id required"}), 400
    if shard_id not in owned_shards:
        return jsonify({"error": f"This server does not own the shard {shard_id}"}), 400

    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM StudT WHERE Stud_id=$1 AND Shard_id=$2", stud_id, shard_id)

    return jsonify({
        "message": f"Data entry with Stud_id:{stud_id} removed",
        "status": "success"
    }), 200


# -------------------- Run --------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
