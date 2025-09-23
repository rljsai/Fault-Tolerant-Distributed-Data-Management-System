import asyncio
import aiohttp
import requests
import matplotlib.pyplot as plt
from collections import Counter
import random

LB_URL = "http://localhost:8000"

def parse_server_id(data):
    msg = data.get("message", "")
    if isinstance(msg, str) and msg.startswith("Hello from server:"):
        return msg.replace("Hello from server:", "").strip()
    return None

async def send_request(session, request_id, path="/home"):
    try:
        async with session.get(f"{LB_URL}{path}?rid={request_id}") as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return parse_server_id(data)
    except Exception:
        return None

async def run_experiment(num_requests=10000):
    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, random.randint(1, 1_000_000)) for _ in range(num_requests)]
        results = await asyncio.gather(*tasks)
    return Counter([r for r in results if r]), sum(r is None for r in results)

def set_servers(n):
    # clear existing
    resp = requests.get(f"{LB_URL}/rep").json()
    existing = resp["message"]["replicas"]
    if existing:
        requests.delete(f"{LB_URL}/rm", json={"n": 9999, "hostnames": existing})

    # add fresh servers
    hostnames = [f"Server{i}" for i in range(1, n + 1)]
    requests.post(f"{LB_URL}/add", json={"n": n, "hostnames": hostnames})

def exp_A2():
    avg_loads = []
    num_servers = list(range(2, 7))  # N = 2 → 6

    for n in num_servers:
        print(f"Running experiment with N={n} servers...")
        set_servers(n)
        counts, failed = asyncio.run(run_experiment())

        total = sum(counts.values())
        avg = total / n if n > 0 else 0
        avg_loads.append(avg)

        print(f"N={n}, Avg load={avg}, Failed={failed}")

    # Plot line chart
    plt.plot(num_servers, avg_loads, marker="o", linestyle="-", color="b")
    plt.title("A2: Average Load per Server (N=2 to 6)")
    plt.xlabel("Number of Servers (N)")
    plt.ylabel("Average Requests per Server")
    plt.grid(True)
    plt.savefig("A2_line_chart.png")
    plt.close()
    print("Chart saved as A2_line_chart.png")

if __name__ == "__main__":
    exp_A2()
