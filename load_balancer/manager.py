from hash_ring import HashRing

class Manager:
    def __init__(self):
        self.ring=HashRing()
        self.replicas=set()

    def add_servers(self,hostnames):
        for h in hostnames:
            if h not in self.replicas:
                self.replicas.add(h)
                self.ring.add_server(h)

    def remove_servers(self,hostnames):
        for h in hostnames:
            if h in self.replicas:
                self.replicas.remove(h)
                self.ring.remove_server(h)

    def list_servers(self):
                return {
            "N": len(self.replicas),
            "replicas": list(self.replicas)
        }
    
    def get_server_for_request(self,path):
         return self.ring.get_server(path)