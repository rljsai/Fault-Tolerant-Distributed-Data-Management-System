import os
from hash_ring import HashRing

class Manager:
    def __init__(self):
        self.ring = HashRing()
        self.replicas = set()
        self.server_ports = {}   # server_id -> port
        self.next_port = 5000    # starting port

    def add_servers(self, hostnames):
        for h in hostnames:
            if h not in self.replicas:
                port = self.next_port
                self.next_port += 1

                # run server container in net1 network
                cmd = (
                    f"docker run -d --rm --name {h} "
                    f"--network net1 --network-alias {h} "
                    f"-e SERVER_ID={h} "
                    f"--label lb=shard_lb "
                    f"myserver"
                )
                print(f"[Manager] Spawning server {h} on port {port}")
                os.system(cmd)

                self.replicas.add(h)
                self.server_ports[h] = port
                self.ring.add_server(h)

    def remove_servers(self, hostnames):
        for h in hostnames:
            if h in self.replicas:
                cmd = f"docker stop {h} && docker rm {h}"
                print(f"[Manager] Stopping server {h}")
                os.system(cmd)

                self.replicas.remove(h)
                self.ring.remove_server(h)
                if h in self.server_ports:
                    del self.server_ports[h]

    def list_servers(self):
        return {
            "N": len(self.replicas),
            "replicas": list(self.replicas),
            "ports": self.server_ports
        }

    def get_server_for_request(self, path):
        return self.ring.get_server(path)

    def get_server_port(self, server_id):
        return self.server_ports.get(server_id, None)
