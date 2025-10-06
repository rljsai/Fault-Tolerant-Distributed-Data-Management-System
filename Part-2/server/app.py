from quart import Quart, jsonify, request, Response
import asyncpg
import os
import asyncio
import sys
from colorama import Fore, Style

app = Quart(__name__)

# -------------------- Environment --------------------
SERVER_ID = os.environ.get("SERVER_ID", "default-server")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.environ.get("POSTGRES_DB", "studdb")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 5432))

db_pool = None
owned_shards = set()


# -------------------- Startup / Shutdown --------------------
@app.before_serving
async def startup():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT
        )

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('''--sql
                    CREATE TABLE IF NOT EXISTS TermT (
                        shard_id TEXT PRIMARY KEY,
                        term INTEGER NOT NULL DEFAULT 0
                    );
                ''')

                await conn.execute('''--sql
                    CREATE TABLE IF NOT EXISTS StudT (
                        stud_id INTEGER NOT NULL,
                        stud_name TEXT NOT NULL,
                        stud_marks INTEGER NOT NULL,
                        shard_id TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        deleted_at INTEGER DEFAULT NULL,
                        PRIMARY KEY (stud_id, created_at),
                        FOREIGN KEY (shard_id) REFERENCES TermT (shard_id)
                    );
                ''')

        print(f"[{SERVER_ID}] ✅ Database initialized successfully")

    except Exception as e:
        print(f'{Fore.RED}ERROR | {e.__class__.__name__}: {e}{Style.RESET_ALL}', file=sys.stderr)
        sys.exit(1)


@app.after_serving
async def shutdown():
    await db_pool.close()


# -------------------- Helper Functions --------------------
async def apply_rules(conn, shard_id, valid_at):
    """
    Rule 1 : delete entries where created_at > vat or (deleted_at is not null and deleted_at <= vat)
    Rule 2 : update deleted_at = null where deleted_at > vat
    """
    await conn.execute('''--sql
        DELETE FROM StudT
        WHERE shard_id = $1
          AND (created_at > $2 OR (deleted_at IS NOT NULL AND deleted_at <= $2));
    ''', shard_id, valid_at)

    await conn.execute('''--sql
        UPDATE StudT
        SET deleted_at = NULL
        WHERE shard_id = $1 AND deleted_at > $2;
    ''', shard_id, valid_at)


# -------------------- Basic endpoints --------------------
@app.route("/home", methods=["GET"])
async def home():
    return jsonify({
        "message": f"Hello from Server: {SERVER_ID}",
        "status": "successful"
    }), 200


@app.route("/heartbeat", methods=["GET"])
async def heartbeat():
    return Response(status=200)


# -------------------- Config --------------------
@app.route("/config", methods=["POST"])
async def config():
    try:
        payload = await request.get_json()
        shards = payload.get("shards", [])
        owned_shards.update(shards)

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                for shard_id in shards:
                    await conn.execute('''--sql
                        INSERT INTO TermT (shard_id, term)
                        VALUES ($1, 0)
                        ON CONFLICT (shard_id) DO NOTHING;
                    ''', shard_id)

        return jsonify({"status": "success"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


# -------------------- Write --------------------
@app.route("/write", methods=["POST"])
async def write():
    try:
        payload = await request.get_json()
        shard_id = payload.get("shard")
        valid_at = int(payload.get("valid_at", -1))
        data = payload.get("data", [])
        admin = str(payload.get("admin", "false")).lower() == "true"

        if shard_id not in owned_shards:
            return jsonify({"status": "error", "message": "Shard not owned"}), 400

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                if admin:
                    term = valid_at
                else:
                    await apply_rules(conn, shard_id, valid_at)
                    term = await conn.fetchval("SELECT term FROM TermT WHERE shard_id=$1", shard_id)
                    term = max(term or 0, valid_at) + 1

                stmt = await conn.prepare('''--sql
                    INSERT INTO StudT (stud_id, stud_name, stud_marks, shard_id, created_at)
                    VALUES ($1, $2, $3, $4, $5);
                ''')
                await stmt.executemany([
                    (row["stud_id"], row["stud_name"], row["stud_marks"], shard_id, term)
                    for row in data
                ])

                await conn.execute("UPDATE TermT SET term=$1 WHERE shard_id=$2", term, shard_id)

        return jsonify({"message": "Data entries added", "valid_at": term, "status": "success"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


# -------------------- Read --------------------
@app.route("/read", methods=["POST"])
async def read():
    try:
        payload = await request.get_json()
        shard_id = payload.get("shard")
        stud_range = payload.get("stud_id", {})
        valid_at = int(payload.get("valid_at", -1))

        low = int(stud_range.get("low", -1))
        high = int(stud_range.get("high", -1))

        if shard_id not in owned_shards:
            return jsonify({"status": "error", "message": "Shard not owned"}), 400

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await apply_rules(conn, shard_id, valid_at)
                rows = await conn.fetch('''--sql
                    SELECT stud_id, stud_name, stud_marks
                    FROM StudT
                    WHERE shard_id=$1
                      AND stud_id BETWEEN $2 AND $3
                      AND created_at <= $4
                      AND (deleted_at IS NULL OR deleted_at > $4);
                ''', shard_id, low, high, valid_at)

        return jsonify({"data": [dict(r) for r in rows], "status": "success"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


# -------------------- Delete --------------------
@app.route("/del", methods=["DELETE"])
async def delete():
    try:
        payload = await request.get_json()
        shard_id = payload.get("shard")
        stud_id = int(payload.get("stud_id", -1))
        valid_at = int(payload.get("valid_at", -1))

        if shard_id not in owned_shards:
            return jsonify({"status": "error", "message": "Shard not owned"}), 400

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await apply_rules(conn, shard_id, valid_at)
                term = await conn.fetchval("SELECT term FROM TermT WHERE shard_id=$1", shard_id)
                term = max(term or 0, valid_at) + 1

                await conn.execute('''--sql
                    UPDATE StudT
                    SET deleted_at=$1
                    WHERE shard_id=$2 AND stud_id=$3 AND created_at <= $4;
                ''', term, shard_id, stud_id, valid_at)

                await conn.execute("UPDATE TermT SET term=$1 WHERE shard_id=$2", term, shard_id)

        return jsonify({
            "message": f"Data entry with stud_id:{stud_id} removed",
            "valid_at": term,
            "status": "success"
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


# -------------------- Update --------------------
@app.route("/update", methods=["POST"])
async def update():
    try:
        payload = await request.get_json()
        shard_id = payload.get("shard")
        valid_at = int(payload.get("valid_at", -1))
        data = payload.get("data", {})
        stud_id = int(payload.get("stud_id", -1))

        if shard_id not in owned_shards:
            return jsonify({"status": "error", "message": "Shard not owned"}), 400

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await apply_rules(conn, shard_id, valid_at)
                term = await conn.fetchval("SELECT term FROM TermT WHERE shard_id=$1", shard_id)
                term = max(term or 0, valid_at) + 1

                # Mark old as deleted
                await conn.execute('''--sql
                    UPDATE StudT
                    SET deleted_at=$1
                    WHERE shard_id=$2 AND stud_id=$3 AND created_at <= $4;
                ''', term, shard_id, stud_id, valid_at)

                # Insert new record
                term += 1
                await conn.execute('''--sql
                    INSERT INTO StudT (stud_id, stud_name, stud_marks, shard_id, created_at)
                    VALUES ($1, $2, $3, $4, $5);
                ''', data["stud_id"], data["stud_name"], data["stud_marks"], shard_id, term)

                await conn.execute("UPDATE TermT SET term=$1 WHERE shard_id=$2", term, shard_id)

        return jsonify({
            "message": f"Data entry for stud_id:{stud_id} updated",
            "valid_at": term,
            "status": "success"
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


# -------------------- Copy --------------------
@app.route("/copy", methods=["POST"])
async def copy():
    try:
        # Read JSON payload
        payload = await request.get_json()
        shards = payload.get("shards", [])
        valid_at = payload.get("valid_at", [])

        if not shards or not valid_at:
            return jsonify({"status": "error", "message": "Missing 'shards' or 'valid_at'"}), 400

        response = {}

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                for shard, vat in zip(shards, valid_at):
                    # Apply cleanup rules before copying
                    await apply_rules(conn, shard, vat)

                    # Fetch rows for this shard and valid_at
                    rows = await conn.fetch('''--sql
                        SELECT stud_id, stud_name, stud_marks, created_at, deleted_at
                        FROM StudT
                        WHERE shard_id = $1
                          AND created_at <= $2;
                    ''', shard, vat)

                    response[shard] = [dict(r) for r in rows]

        response["status"] = "success"
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


# -------------------- Run --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, use_reloader=False)
