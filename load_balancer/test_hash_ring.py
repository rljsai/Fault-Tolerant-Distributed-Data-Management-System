from hash_ring import HashRing
def test_distribution(ring, num_requests):
    counts = {s: 0 for s in ring.get_servers()}
    for req in range(1, num_requests + 1):
        server = ring.get_server(req)
        counts[server] += 1

    print(f"\nRequest distribution for {num_requests} requests:")
    total = sum(counts.values())
    for server, count in counts.items():
        percent = (count / total) * 100
        print(f"  Server {server}: {count} requests ({percent:.2f}%)")

def main():
    ring = HashRing()

    # Add servers
    print("Adding servers...")
    for s in [1, 2, 3]:
        ring.add_server(s)
    print("Active servers:", ring.get_servers())

    # Small sample (50 requests)
    test_distribution(ring, 50)

    # Larger samples
    test_distribution(ring, 500)
    test_distribution(ring, 5000)

    # Remove one server
    print("\nRemoving server 2...")
    ring.remove_server(2)
    print("Active servers:", ring.get_servers())

    # Check again after removal
    test_distribution(ring, 500)
    test_distribution(ring, 5000)

if __name__ == "__main__":
    main()