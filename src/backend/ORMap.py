from collections import defaultdict
from copy import deepcopy
import json


class DotContext:
    def __init__(self):
        #self.dots = defaultdict(set)
        self.dots =  {}

    def add_dot(self, actor_id, dot):
        if actor_id not in self.dots: 
            self.dots[actor_id] = set()
        self.dots[actor_id].add(dot)

    def join(self, other_context):
        for actor_id, dots in other_context.dots.items():
            if actor_id not in self.dots: 
                self.dots[actor_id] = set()
            self.dots[actor_id].update(dots)
    
    def to_dict(self):
        return {actor_id: list(dots) for actor_id, dots in self.dots.items()}
    
    @classmethod
    def from_dict(cls, data):
        obj = cls()
        print(1)
        # obj.dots = defaultdict(set, {actor_id: set(dots) for actor_id, dots in data.items()})
        obj.dots = {actor_id: set(dots) for actor_id, dots in data.items()}
        print(f"The dots are {obj.dots}")
        return obj


class ORMap:
    def __init__(self, actor_id, shared_context=None):
        #self.items = {}  
        #self.tombstones = {}
        #self.actor_id = actor_id
        #self.context = shared_context if shared_context else DotContext()
        self.obj = {"items" : {}, "tombstones": {}, "context": shared_context if shared_context else DotContext(), "actor_id": actor_id}

    def add_item(self, key, clientPNCounter):
        if key not in self.obj["items"]:
            self.obj["items"][key] = set()
        if self.obj["actor_id"] not in self.obj["context"].dots: 
            self.obj["context"].dots[self.obj["actor_id"]] = set()
        dot = f"{self.obj["actor_id"]}:{len(self.obj["context"].dots[self.obj["actor_id"]]) + 1}"
        self.obj["items"][key].add(dot)
        self.obj["context"].add_dot(self.obj["actor_id"], dot)
        print(f"The clientPNCounter is {clientPNCounter.obj}")
        clientPNCounter = clientPNCounter.setup_item(key,self.obj["actor_id"])
        # If the key was previously deleted remove its tombstone
        if key in self.obj["tombstones"]:
            del self.obj["tombstones"][key]
        
        return self, clientPNCounter
    
    def add_list(self, key):
        if key not in self.obj["items"]:
            self.obj["items"][key] = set()
        if self.obj["actor_id"] not in self.obj["context"].dots: 
            self.obj["context"].dots[self.obj["actor_id"]] = set()
        dot = f"{self.obj["actor_id"]}:{len(self.obj["context"].dots[self.obj["actor_id"]]) + 1}"
        self.obj["items"][key].add(dot)
        self.obj["context"].add_dot(self.obj["actor_id"], dot)
        # If the key was previously deleted remove its tombstone
        if key in self.obj["tombstones"]:
            del self.obj["tombstones"][key]
        
        return self


    def delete_item(self, key, clientPNCounter):
        if key in self.obj["items"]:
            # Move all dots to the tombstone set
            if key not in self.obj["tombstones"]:
                self.obj["tombstones"][key] = set()
            self.obj["tombstones"][key].update(self.obj["items"][key])
            del self.obj["items"][key]
            clientPNCounter = clientPNCounter.remove_item(key)

            return self, clientPNCounter
        
    def delete_list(self,key): 
        if key in self.obj["items"]: 
            if key not in self.obj["tombstones"]: 
                self.obj["tombstones"][key] = set()
            self.obj["tombstones"][key].update(self.obj["items"][key])
            del self.obj["items"][key]

        return self

    def join(self, currentList, otherORMap):

        for key, dots in otherORMap.obj["items"].items():
            print(f"The dots are {dots}")
            print(f"the key is {key}")
            print(f"The self is {self}")
            print(f"The self object is {self.obj}")
            if key in self.obj["items"]:
                print(1)
                self.obj["items"][key].update(dots)
                print(2)
            elif key not in self.obj["tombstones"]:  # If no tombstone exists, include the item
                print(3)
                self.obj["items"][key] = deepcopy(dots)
                print(4)
            elif key in self.obj["tombstones"]:
                print(5)
                for dot in dots:
                    print(6)
                    if dot not in self.obj["tombstones"][key]:
                        print(7)
                        if key not in self.obj["items"]:
                            print(8)
                            self.obj["items"][key] = set()
                        print(9)
                        self.obj["items"][key].add(dot)


        # Merge tombstones
        print(10)
        for key, dots in otherORMap.obj["tombstones"].items():
            print(11)
            if key not in self.obj["tombstones"]:
                print(12)
                self.obj["tombstones"][key] = set()
            print(13)
            self.obj["tombstones"][key].update(dots)


        # Remove any items that are now deleted
        print(14)
        for key in list(self.obj["items"].keys()):
            # Check ALL dots in the tombstone set
            print(15)
            if key in self.obj["tombstones"] and self.obj["tombstones"][key].issuperset(self.obj["items"][key]):
                print(16)
                del self.obj["items"][key]

        # Merge causal contexts
        print(18)
        self.obj["context"].join(otherORMap.obj["context"])
        print(19)
        
        print(f"The current list items are {currentList['items']}")
        delete_keys = []
        for key in list(currentList["items"].keys()):
            print(20)
            print(f"The key outside is {key}")
            if key not in self.obj["items"]:
                print(f"The key is {key}") 
                print(21)
                del currentList["items"][key]
                print(22)

        print(f"Reached here")

        print(f"The current list items are {currentList["items"]}")
        print(f"The ORMap items are {self.obj}")
            
        return self, currentList["items"]
    

    def join_lists(self, clientORMapLists):

        print(f"The clientORMapLists are {clientORMapLists}")
        for key, dots in clientORMapLists.obj["items"].items():
            print(f"The dots are {dots}")
            print(f"the key is {key}")
            print(f"The self is {self}")
            print(f"The self object is {self.obj}")
            if key in self.obj["items"]:
                print(1)
                self.obj["items"][key].update(dots)
                print(2)
            elif key not in self.obj["tombstones"]:  # If no tombstone exists, include the item
                print(3)
                self.obj["items"][key] = deepcopy(dots)
                print(4)
            elif key in self.obj["tombstones"]:
                print(5)
                for dot in dots:
                    print(6)
                    if dot not in self.obj["tombstones"][key]:
                        print(7)
                        if key not in self.obj["items"]:
                            print(8)
                            self.obj["items"][key] = set()
                        print(9)
                        self.obj["items"][key].add(dot)


        # Merge tombstones
        print(10)
        for key, dots in clientORMapLists.obj["tombstones"].items():
            print(11)
            if key not in self.obj["tombstones"]:
                print(12)
                self.obj["tombstones"][key] = set()
            print(13)
            self.obj["tombstones"][key].update(dots)


        # Remove any items that are now deleted
        print(14)
        for key in list(self.obj["items"].keys()):
            # Check ALL dots in the tombstone set
            print(15)
            if key in self.obj["tombstones"] and self.obj["tombstones"][key].issuperset(self.obj["items"][key]):
                print(16)
                del self.obj["items"][key]

        # Merge causal contexts
        print(18)
        self.obj["context"].join(clientORMapLists.obj["context"])
        print(19)

        print(f"Reached here")

        print(f"The current list items are {clientORMapLists.obj["items"]}")
        print(f"The ORMap items are {self.obj}")
            
        return self
    

    def to_dict(self):
        return {
            "items": {key: list(dots) for key, dots in self.obj["items"].items()},
            "tombstones": {key: list(dots) for key, dots in self.obj["tombstones"].items()},
            "context": self.obj["context"].to_dict(),
            "actor_id": self.obj["actor_id"]
        }
    
    
    @classmethod
    def from_dict(cls, data,ident):
        print(f"Data is {data}")
        orMapInstance = cls(ident)
        print(1)
        orMapInstance.obj["items"] = {key: set(dots) for key, dots in data.get("items", {}).items()}
        orMapInstance.obj["tombstones"] = {key: set(dots) for key, dots in data.get("tombstones", {}).items()}
        orMapInstance.obj["context"] = DotContext.from_dict(data.get("context", {}))
        return orMapInstance
