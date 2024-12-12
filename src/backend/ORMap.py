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
        # obj.dots = defaultdict(set, {actor_id: set(dots) for actor_id, dots in data.items()})
        obj.dots = {actor_id: set(dots) for actor_id, dots in data.items()}
        return obj


class ORMap:
    def __init__(self, actor_id, shared_context=None):
        self.items = {}  
        self.tombstones = {}
        self.actor_id = actor_id
        self.context = shared_context if shared_context else DotContext()
    
    def add_item(self, key):
        if key not in self.items:
            self.items[key] = set()
        if self.actor_id not in self.context.dots: 
            self.context.dots[self.actor_id] = set()
        dot = f"{self.actor_id}:{len(self.context.dots[self.actor_id]) + 1}"
        self.items[key].add(dot)
        self.context.add_dot(self.actor_id, dot)

        # If the key was previously deleted remove its tombstone
        if key in self.tombstones:
            del self.tombstones[key]

    def delete_item(self, key):
        if key in self.items:
            # Move all dots to the tombstone set
            if key not in self.tombstones:
                self.tombstones[key] = set()
            self.tombstones[key].update(self.items[key])
            del self.items[key]

    def join(self, other):
        # Merge items
        if isinstance(other, dict):
            other = ORMap.from_dict(other)

        print(f"The other is {other}")
        print(f"Other items are {other.items}")
        print(f"Other tombstones are {other.tombstones}")
        print(f"Other context is {other.context}")
        for key, dots in other.items.items():
            print(f"The dots are {dots}")
            print(f"the key is {key}")
            if key in self.items:
                self.items[key].update(dots)
            elif key not in self.tombstones:  # If no tombstone exists, include the item
                self.items[key] = deepcopy(dots)
            elif key in self.tombstones:
                for dot in dots:
                    if dot not in self.tombstones[key]:
                        if key not in self.items:
                            self.items[key] = set()
                        self.items[key].add(dot)

        # Merge tombstones
        for key, dots in other.tombstones.items():
            if key not in self.tombstones:
                self.tombstones[key] = set()
            self.tombstones[key].update(dots)


        # Remove any items that are now deleted
        for key in list(self.items.keys()):
            # Check ALL dots in the tombstone set
            if key in self.tombstones and self.tombstones[key].issuperset(self.items[key]):
                del self.items[key]

        # Merge causal contexts
        self.context.join(other.context)
        return self
    

    def to_dict(self):
        return {
            "items": {key: list(dots) for key, dots in self.items.items()},
            "tombstones": {key: list(dots) for key, dots in self.tombstones.items()},
            "context": self.context.to_dict(),
            "actor_id": self.actor_id
        }
    
    @classmethod
    def from_dict(cls, data):
        print(f"Data is {data}")
        obj = cls(0)
        obj.items = {key: set(dots) for key, dots in data.get("items", {}).items()}
        obj.tombstones = {key: set(dots) for key, dots in data.get("tombstones", {}).items()}
        obj.context = DotContext.from_dict(data.get("context", {}))
        return obj
