import hashlib
import bisect
import math

class HashRing:
    def __init__(self, total_slots=512):
        self.total_slots = total_slots
        self.ring = {}                       # slot → server
        self.sorted_slots = []               # sorted slot keys
        self.servers = set()                 # track active servers
        self.K = int(math.log2(total_slots)) # number of virtual nodes per server

    def _request_hash(self, i):
        """Hash function for request IDs using MD5"""
        h = hashlib.md5(str(i).encode()).hexdigest()
        return int(h, 16) % self.total_slots

    def _virtual_server_hash(self, server_id, replica_id):
        """Hash function for virtual servers using MD5"""
        key = f"server-{server_id}-replica-{replica_id}"
        h = hashlib.md5(key.encode()).hexdigest()
        return int(h, 16) % self.total_slots

    def add_server(self, server_id):
        """Add a new server with virtual nodes using linear probing"""
        if server_id in self.servers:
            return
        self.servers.add(server_id)
        for j in range(self.K):
            slot = self._virtual_server_hash(server_id, j)
            start_slot = slot
            # Linear probing to find an empty slot
            while slot in self.ring:
                slot = (slot + 1) % self.total_slots
                if slot == start_slot:
                    raise Exception("Hash ring is full!")
            self.ring[slot] = server_id
            bisect.insort(self.sorted_slots, slot)

    def remove_server(self, server_id):
        """Remove server and its virtual nodes"""
        if server_id not in self.servers:
            return
        self.servers.remove(server_id)
        # Remove only slots belonging to this server
        slots_to_remove = [slot for slot, sid in self.ring.items() if sid == server_id]
        for slot in slots_to_remove:
            del self.ring[slot]
            self.sorted_slots.remove(slot)

    def get_server(self, request_id):
        """Find nearest clockwise server for request"""
        if not self.sorted_slots:
            return None
        slot = self._request_hash(request_id)
        idx = bisect.bisect_left(self.sorted_slots, slot)
        if idx == len(self.sorted_slots):
            idx = 0  # wrap around
        nearest_slot = self.sorted_slots[idx]
        return self.ring[nearest_slot]
    
    def get_next_server(self, current_server):
        """Return the next clockwise server after current_server"""
        if not self.sorted_slots:
            return None

        slots = [slot for slot, sid in self.ring.items() if sid == current_server]
        if not slots:
            return None

        start_slot = slots[0]
        idx = self.sorted_slots.index(start_slot)

        for i in range(1, len(self.sorted_slots) + 1):
            next_idx = (idx + i) % len(self.sorted_slots)
            next_server = self.ring[self.sorted_slots[next_idx]]
            if next_server != current_server:
                return next_server
        return None

    def get_servers(self):
        """Return list of active servers"""
        return list(self.servers)
