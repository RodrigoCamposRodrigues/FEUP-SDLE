from collections import defaultdict
from copy import deepcopy
import json


class DotContext:
    def __init__(self):
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
        obj.dots = {actor_id: set(dots) for actor_id, dots in data.items()}
        return obj


class ORMap:
    def __init__(self, actor_id, shared_context=None):
        self.obj = {'items' : {}, "tombstones": {}, 'context': shared_context if shared_context else DotContext(), 'actor_id': actor_id}

    def add_item(self, key, clientPNCounter):
        if key not in self.obj['items']:
            self.obj['items'][key] = set()
        if self.obj['actor_id'] not in self.obj['context'].dots: 
            self.obj['context'].dots[self.obj['actor_id']] = set()
        dot = f"{self.obj['actor_id']}:{len(self.obj['context'].dots[self.obj['actor_id']]) + 1}"
        self.obj['items'][key].add(dot)
        self.obj['context'].add_dot(self.obj['actor_id'], dot)
        clientPNCounter = clientPNCounter.setup_item(key,self.obj['actor_id'])
        # If the key was previously deleted remove its tombstone
        if key in self.obj["tombstones"]:
            del self.obj["tombstones"][key]
        
        return self, clientPNCounter
    
    def add_list(self, key):
        if key not in self.obj['items']:
            self.obj['items'][key] = set()
        if self.obj['actor_id'] not in self.obj['context'].dots: 
            self.obj['context'].dots[self.obj['actor_id']] = set()
        dot = f"{self.obj['actor_id']}:{len(self.obj['context'].dots[self.obj['actor_id']]) + 1}"
        self.obj['items'][key].add(dot)
        self.obj['context'].add_dot(self.obj['actor_id'], dot)
        # If the key was previously deleted remove its tombstone
        if key in self.obj["tombstones"]:
            del self.obj["tombstones"][key]
        
        return self


    def delete_item(self, key, clientPNCounter):
        if key in self.obj['items']:
            # Move all dots to the tombstone set
            if key not in self.obj["tombstones"]:
                self.obj["tombstones"][key] = set()
            self.obj["tombstones"][key].update(self.obj['items'][key])
            del self.obj['items'][key]
            clientPNCounter = clientPNCounter.remove_item(key)

            return self, clientPNCounter
        
    def delete_list(self,key): 
        if key in self.obj['items']: 
            if key not in self.obj["tombstones"]: 
                self.obj["tombstones"][key] = set()
            self.obj["tombstones"][key].update(self.obj['items'][key])
            del self.obj['items'][key]

        return self

    def join(self, currentList, otherORMap):

        for key, dots in otherORMap.obj['items'].items():
            if key in self.obj['items']:
                self.obj['items'][key].update(dots)
            elif key not in self.obj["tombstones"]:  # If no tombstone exists, include the item
                self.obj['items'][key] = deepcopy(dots)
            elif key in self.obj["tombstones"]:
                for dot in dots:
                    if dot not in self.obj["tombstones"][key]:
                        if key not in self.obj['items']:
                            self.obj['items'][key] = set()
                        self.obj['items'][key].add(dot)


        # Merge tombstones
        for key, dots in otherORMap.obj["tombstones"].items():
            if key not in self.obj["tombstones"]:
                self.obj["tombstones"][key] = set()
            self.obj["tombstones"][key].update(dots)


        # Remove any items that are now deleted
        for key in list(self.obj['items'].keys()):
            # Check ALL dots in the tombstone set
            if key in self.obj["tombstones"] and self.obj["tombstones"][key].issuperset(self.obj['items'][key]):
                del self.obj['items'][key]

        # Merge causal contexts
        self.obj['context'].join(otherORMap.obj['context'])
        
        delete_keys = []
        for key in list(currentList['items'].keys()):
            if key not in self.obj['items']:
                del currentList['items'][key]
            
        return self, currentList['items']
    

    def join_lists(self, clientORMapLists):

        for key, dots in clientORMapLists.obj['items'].items():
            if key in self.obj['items']:
                self.obj['items'][key].update(dots)
            elif key not in self.obj["tombstones"]:  # If no tombstone exists, include the item
                self.obj['items'][key] = deepcopy(dots)
            elif key in self.obj["tombstones"]:
                for dot in dots:
                    if dot not in self.obj["tombstones"][key]:
                        if key not in self.obj['items']:
                            self.obj['items'][key] = set()
                        self.obj['items'][key].add(dot)


        # Merge tombstones
        for key, dots in clientORMapLists.obj["tombstones"].items():
            if key not in self.obj["tombstones"]:
                self.obj["tombstones"][key] = set()
            self.obj["tombstones"][key].update(dots)


        # Remove any items that are now deleted
        for key in list(self.obj['items'].keys()):
            # Check ALL dots in the tombstone set
            if key in self.obj["tombstones"] and self.obj["tombstones"][key].issuperset(self.obj['items'][key]):
                del self.obj['items'][key]

        # Merge causal contexts
        self.obj['context'].join(clientORMapLists.obj['context'])
            
        return self
    

    def to_dict(self):
        return {
            'items': {key: list(dots) for key, dots in self.obj['items'].items()},
            "tombstones": {key: list(dots) for key, dots in self.obj["tombstones"].items()},
            'context': self.obj['context'].to_dict(),
            'actor_id': self.obj['actor_id']
        }
    
    
    @classmethod
    def from_dict(cls, data,ident):
        orMapInstance = cls(ident)
        orMapInstance.obj['items'] = {key: set(dots) for key, dots in data.get('items', {}).items()}
        orMapInstance.obj["tombstones"] = {key: set(dots) for key, dots in data.get("tombstones", {}).items()}
        orMapInstance.obj['context'] = DotContext.from_dict(data.get('context', {}))
        return orMapInstance