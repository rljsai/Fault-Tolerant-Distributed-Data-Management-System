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

def plot_results(name, counts, failed):
    total = sum(counts.values())
    plt.bar(counts.keys(), counts.values())

    # add % labels
    for i, (server, count) in enumerate(counts.items()):
        plt.text(i, count, f"{count/total:.2%}", ha="center", va="bottom")

    plt.title(f"{name}: Load distribution")
    plt.xlabel("Server")
    plt.ylabel("Requests handled")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.figtext(0.99, 0.01, f"Failed requests: {failed}", ha="right", fontsize=8)
    plt.savefig(f"{name}_bar_chart.png")
    plt.close()
    print(f"{name} Results:", counts, "| Failed:", failed)

def exp_A1():
    set_servers(3)
    counts, failed = asyncio.run(run_experiment())
    plot_results("A1", counts, failed)

if __name__ == "__main__":
    exp_A1()
    print("Charts saved as A1_bar_chart.png and A2_bar_chart.png")
