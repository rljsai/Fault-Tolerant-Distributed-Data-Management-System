import asyncio
import aiohttp
import requests
import matplotlib.pyplot as plt
from collections import Counter
import random
import time

LB_URL = "http://localhost:8000"

def parse_server_id(data):
    msg = data.get("message", "")
    if msg.startswith("Hello from server:"):
        return msg.replace("Hello from server:", "").strip()
    return None

async def send_request(session, request_id, path="/home"):
    async with session.get(f"{LB_URL}{path}?rid={request_id}") as resp:
        if resp.status != 200:
            return None
        data = await resp.json()
        return parse_server_id(data)

async def run_experiment(num_requests=1000): # only 1000 requests
    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, random.randint(1, 1_000_000)) for _ in range(num_requests)]
        results = await asyncio.gather(*tasks)
    return Counter([r for r in results if r])

def set_servers(n):
    # clear all
    resp = requests.get(f"{LB_URL}/rep").json()
    existing = resp["message"]["replicas"]
    if existing:
        requests.delete(f"{LB_URL}/rm", json={"n": len(existing), "hostnames": existing})

    # add clean Server1..N
    hostnames = [f"Server{i}" for i in range(1, n + 1)]
    requests.post(f"{LB_URL}/add", json={"n": n, "hostnames": hostnames})
    time.sleep(3)

def exp_A1():
    set_servers(3)
    counts = asyncio.run(run_experiment())
    print("A-1 Results:", counts)

    plt.bar(counts.keys(), counts.values())
    plt.title("A-1: Load distribution with N=3 servers")
    plt.xlabel("Server")
    plt.ylabel("Requests handled")
    plt.savefig("A1_bar_chart.png")
    plt.close()

def exp_A2():
    for n in [4,6]:
        print(f"\nRunning experiment with N={n}")
        set_servers(n)
        counts = asyncio.run(run_experiment())
        print(f"A-2 (N={n}) Results:", counts)

        plt.bar(counts.keys(), counts.values())
        plt.title(f"A-2: Load distribution with N={n} servers")
        plt.xlabel("Server")
        plt.ylabel("Requests handled")
        plt.savefig(f"A2_N{n}_bar_chart.png")
        plt.close()

if __name__ == "__main__":
    exp_A2()
    print("✅ Chart saved as A2_bar_chart.png")


