from hash_ring import HashRing
from collections import Counter
import random

# Create ring with 3 servers
ring = HashRing(total_slots=512)
ring.add_server("Server1")
ring.add_server("Server2")
ring.add_server("Server3")

# Generate 10,000 random request IDs
results = []
for _ in range(10000):
    rid = random.randint(1, 1_000_000)
    server = ring.get_server(rid)
    results.append(server)

# Count how many requests each server handled
counts = Counter(results)
print("Request distribution over 10,000 requests:")
for server, count in counts.items():
    print(f"{server}: {count}")
