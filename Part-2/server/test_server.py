import asyncio
import aiohttp
import json

BASE_URL = "http://localhost:5000"

async def main():
    async with aiohttp.ClientSession() as session:

        print("1️⃣ Configuring shards...")
        async with session.post(f"{BASE_URL}/config", json={"shards": ["sh1"]}) as resp:
            print(await resp.json())

        print("\n2️⃣ Writing data...")
        data_to_write = [
            {"stud_id": 101, "stud_name": "Alice", "stud_marks": 85},
            {"stud_id": 102, "stud_name": "Bob", "stud_marks": 90},
        ]
        async with session.post(f"{BASE_URL}/write", json={
            "shard": "sh1",
            "data": data_to_write,
            "valid_at": 0,
            "admin": True
        }) as resp:
            print(await resp.json())

        print("\n3️⃣ Reading data...")
        async with session.post(f"{BASE_URL}/read", json={
            "shard": "sh1",
            "stud_id": {"low": 100, "high": 200},
            "valid_at": 0
        }) as resp:
            print(await resp.json())

        print("\n4️⃣ Copying shard...")
        resp = await session.post(
              f"{BASE_URL}/copy",
              json={"shards": ["sh1"], "valid_at": [0]}
        )
        print(await resp.json())


        print("\n5️⃣ Updating data for stud_id=101...")
        async with session.post(f"{BASE_URL}/update", json={
            "shard": "sh1",
            "stud_id": 101,
            "data": {"stud_id": 101, "stud_name": "AliceUpdated", "stud_marks": 95},
            "valid_at": 1
        }) as resp:
            print(await resp.json())

        print("\n6️⃣ Reading updated data...")
        async with session.post(f"{BASE_URL}/read", json={
            "shard": "sh1",
            "stud_id": {"low": 100, "high": 200},
            "valid_at": 2
        }) as resp:
            print(await resp.json())

        print("\n7️⃣ Deleting data for stud_id=102...")
        async with session.delete(f"{BASE_URL}/del", json={
            "shard": "sh1",
            "stud_id": 102,
            "valid_at": 3
        }) as resp:
            print(await resp.json())

        print("\n8️⃣ Final read after delete...")
        async with session.post(f"{BASE_URL}/read", json={
            "shard": "sh1",
            "stud_id": {"low": 100, "high": 200},
            "valid_at": 4
        }) as resp:
            print(await resp.json())

asyncio.run(main())
