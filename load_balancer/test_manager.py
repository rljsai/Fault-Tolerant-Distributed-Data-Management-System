from manager import Manager

def main():
    mgr = Manager()

    # Add servers
    mgr.add_servers(["S1", "S2", "S3"])
    print("After adding servers:", mgr.list_servers())

    # Route some requests
    for req in ["/home", "/heartbeat", "/user/123", "/profile"]:
        server = mgr.get_server_for_request(req)
        print(f"Request {req} → {server}")

    # Remove one server
    mgr.remove_servers(["S2"])
    print("After removing S2:", mgr.list_servers())

    # Route again
    for req in ["/home", "/heartbeat", "/user/123", "/profile"]:
        server = mgr.get_server_for_request(req)
        print(f"Request {req} → {server}")

if __name__ == "__main__":
    main()
