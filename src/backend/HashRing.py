import hashlib
import bisect

class HashRing:
    def __init__(self, nodes=None):
        self.virtual_nodes = 3
        self.ring = {}
        self.sorted_keys = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key):
        return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16)
    
    def add_node(self, node):
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}-{i}"
            hashed_key = self._hash(virtual_node_key)
            self.ring[hashed_key] = node
            bisect.insort(self.sorted_keys, hashed_key)
    
    def remove_node(self, node):
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}-{i}"
            hashed_key = self._hash(virtual_node_key)
            if hashed_key in self.ring:
                self.ring.pop(hashed_key)
                self.sorted_keys.remove(hashed_key)
    
    def get_node(self, key):
        if not self.ring:
            return None
        
        hashed_key = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hashed_key)
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]
    
    def get_preference_list(self, key, num_replicas = 3):
        print(f"Ring: {len(self.ring)}")
        if not self.ring:
            return None
        
        if len(self.ring) == 6:
            num_replicas = 2
        elif len(self.ring) == 3:
            num_replicas = 1
        
        hashed_key = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hashed_key)

        p_list = []
        visited_nodes = set()

        while len(p_list) < num_replicas and len(visited_nodes) < len(self.sorted_keys):
            if idx == len(self.sorted_keys):
                idx = 0

            node = self.ring[self.sorted_keys[idx]]
            
            if (node not in visited_nodes):
                p_list.append(node)
                visited_nodes.add(node)

            idx += 1

        return p_list


    def get_nodes(self):
        return list(set(self.ring.values()))


 