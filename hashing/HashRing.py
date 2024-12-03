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
    
    def get_nodes(self):
        return list(set(self.ring.values()))


""" 
test test 
def main():
    list_key1 = "abc"
    list_key2 = "def"
    list_key3 = "ghi"
    list_key4 = "jkl"
    list_key5 = "mno"
    list_key6 = "pqr"
    list_key7 = "stu"

    nodes = ["node1", "node2", "node3"]

    ring = HashRing(nodes)

    print("normal")

    print(ring.get_node(list_key1))
    print(ring.get_node(list_key2))
    print(ring.get_node(list_key3))
    print(ring.get_node(list_key4))
    print(ring.get_node(list_key5))
    print(ring.get_node(list_key6))
    print(ring.get_node(list_key7))

    ring.remove_node("node2")

    print("reomved 2")


    print(ring.get_node(list_key1))
    print(ring.get_node(list_key2))
    print(ring.get_node(list_key3))
    print(ring.get_node(list_key4))
    print(ring.get_node(list_key5))
    print(ring.get_node(list_key6))
    print(ring.get_node(list_key7))

    print("added node 4")


    ring.add_node("node4")
    print(ring.get_node(list_key1))
    print(ring.get_node(list_key2))
    print(ring.get_node(list_key3))
    print(ring.get_node(list_key4))
    print(ring.get_node(list_key5))
    print(ring.get_node(list_key6))
    print(ring.get_node(list_key7))

    print("removed node 1 and 3")

    ring.remove_node("node1")
    ring.remove_node("node3")

    print(ring.get_node(list_key1))
    print(ring.get_node(list_key2))
    print(ring.get_node(list_key3))
    print(ring.get_node(list_key4))
    print(ring.get_node(list_key5))
    print(ring.get_node(list_key6))
    print(ring.get_node(list_key7))


if __name__ == "__main__":
    main()




 """