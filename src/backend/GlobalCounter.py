from collections import defaultdict
from ORMap import ORMap, DotContext
class GlobalCounter:

    shopping_lists = {} 
    orMaps = {}

    def __init__(self, uuid, lista): 
        self.list = lista
        self.id = uuid
        self.shopping_lists[self.id] = self.list
        # Create a dictionary of dictionaries
        self.crdt_states = {}
        self.orMaps = {}
        # Start the vector clocks with 0 for all the items in the list where the index 0 is A and index 1 is B and so on
        self.crdt_states['PNCounter'] = {}
        for item in self.list["items"]:
            self.list['crdt_states']['PNCounter'][item] = {}
        

    def to_dict(self):
        return {"id": self.id, "list": self.list, 'crdt_states' : self.crdt_states}
    
    def add_item(self, item, listId,clientId,orMapsOther): 
        if item not in self.list["items"]:
            self.list["items"][item] = 0
            self.list['crdt_states']['PNCounter'][item] = {}
            orMapsOther.add_item(item)
            if listId not in self.orMaps: 
                self.orMaps[listId] = {}
            self.orMaps[listId] = orMapsOther
        else: 
            raise KeyError(f"Item '{item}' already exists in the shopping list.")
        print(f"The self is {self.list}")
        return orMapsOther
        
    def remove_item(self, item ,listId ,clientId, orMapsOther): 
        if item in self.list["items"]: 
            print(f"The orMapsOther are {orMapsOther}")
            print(f"The self list is {self.list}")
            orMapsOther.delete_item(item)
            del self.list["items"][item]
            del self.list['crdt_states']['PNCounter'][item]
            if listId not in self.orMaps: 
                self.orMaps[listId] = {}
            self.orMaps[listId] = orMapsOther

        return orMapsOther
    
    def add_new_item_quantity(self, item, quantity): 
        # Initialize the array with the item and quantity
        self.list[item] = quantity
        
    def get_shopping_list(self): 
        return self
    
    def increment_value(self,id, item):

        if item in self.list["items"]:
            if(item not in self.list['crdt_states']['PNCounter']): 
                self.list['crdt_states']['PNCounter'][item] = {} 
            if(str(id) not in self.list['crdt_states']['PNCounter'][item].keys()):
                print(f"Id {id} not found in the list")
                self.list['crdt_states']['PNCounter'][item][id] = {}
                self.list['crdt_states']['PNCounter'][item][id]["inc"] = 1
                self.list['crdt_states']['PNCounter'][item][id]["dec"] = 0
                self.list["items"][item] += 1
            else: 
                self.list['crdt_states']['PNCounter'][item][id]["inc"] += 1
                self.list["items"][item] += 1
        else: 
            raise KeyError(f"Item '{item}' not found in the shopping list.")
        


    def decrement_value(self,id,item):
        if item in self.list["items"]:
            if item not in self.list['crdt_states']['PNCounter']: 
                self.list['crdt_states']['PNCounter'][item] = {} 
            if id not in self.list['crdt_states']['PNCounter'][item].keys():
                self.list['crdt_states']['PNCounter'][item][id] = {}
                self.list['crdt_states']['PNCounter'][item][id]["inc"] = 0
                self.list['crdt_states']['PNCounter'][item][id]["dec"] = 1
                self.list["items"][item] -= 1
            else: 
                self.list['crdt_states']['PNCounter'][item][id]["dec"] += 1
                self.list["items"][item] -= 1
        else:
            raise KeyError(f"Item '{item}' not found in the shopping list.")
        

    def compare_version(self, version2):
        for item in self.list["items"]: 
            if item in version2.list["items"] and self.list["items"][item] > version2.list["items"][item]:
                return 1
            elif item in version2.list["items"] and self.list["items"][item] < version2.list["items"][item]:
                return -1
        return 0

    def merge_version(self, version2, crdt_state, orMapsOther):
        # Treat them always as ormaps, just convert them to dict when sending
        if (type(self.list['crdt_states']['ORMap']) == dict):
            self.list['crdt_states']['ORMap'] = ORMap.from_dict(self.list['crdt_states']['ORMap'])
        print(f"The crdt_state inside is {crdt_state}")
        print(f"The self orMaps are {self.list['crdt_states']}")
        print(f"The other orMaps are {orMapsOther}")
        print(f"The type of the self orMaps are {type(self.list['crdt_states']['ORMap'])}")
        print(f"The type of the ORMap are {type(orMapsOther)}")
        self.orMaps[version2["id"]] = self.list['crdt_states']['ORMap']
        if self.orMaps == {}: 
            print(1)
            self.orMaps[version2["id"]] = {}
            for clientId in orMapsOther.keys():
                print(2)
                self.orMaps[version2["id"]] = orMapsOther
        elif self.orMaps != {}:
            print(3) 
            # Merge the orMaps using the join function in the ORMap class 
            print(f"The self orMaps after 3 are {self.orMaps}")
            #self.orMaps[version2["id"]].join(orMapsOther)
            self.list['crdt_states']['ORMap'].join(orMapsOther)
            print(4)
            print(f"The version2 is {crdt_state}")
            print(f"The self is {self.list}")
            print(f"The total list is {self.list}")
            for item in crdt_state['PNCounter']:
                print(5)
                current_sum_inc = 0
                current_sum_dec = 0 
                if item not in self.list['crdt_states']['PNCounter']:
                    print(6)
                    # self.crdt_states['PNCounter'][item] = {}
                    self.list['crdt_states']['PNCounter'][item] = {}
                merged_value = 0
                print(7)
                if(crdt_state['PNCounter'][item] != {} or self.list['crdt_states']['PNCounter'][item] != {}): 
                    # join the keys from the crdt_state[item] and the self.crdt_state[item]
                    print(8)
                    crdt_state_item_keys = set(crdt_state['PNCounter'].get(item, {}).keys())
                    self_crdt_states_item_keys = set(self.list['crdt_states']['PNCounter'].get(item, {}).keys())
                    print(9)
                    all_keys = crdt_state_item_keys.union(self_crdt_states_item_keys)
                    print(f"The all keys are {all_keys}")
                    for clientId in all_keys:
                        print(10)
                        print(f"The self.list['crdt_states']['PNCounter'] is {self.list['crdt_states']['PNCounter']}")
                        if item not in self.list['crdt_states']['PNCounter']: 
                            print(11)
                            self.list['crdt_states']['PNCounter'][item] = {}
                        if clientId not in self.list['crdt_states']['PNCounter'][item]:
                            print(11.5)
                            self.list['crdt_states']['PNCounter'][item][clientId] = {"inc": 0, "dec": 0}
                            print(f"The self.list['crdt_states']['PNCounter'] is {self.list['crdt_states']['PNCounter']}")
                        print(11.6)
                        print(f"The crdt_state is {crdt_state}")
                        if clientId not in crdt_state['PNCounter'][item]:
                            print(12)
                            crdt_state['PNCounter'][item][clientId] = {"inc": 0, "dec": 0}
                        # Get always the max value between the versions
                        print(13)
                        crdt_state['PNCounter'][item][clientId]["inc"] = max(crdt_state['PNCounter'][item][clientId]["inc"],self.list['crdt_states']['PNCounter'][item][clientId]["inc"])
                        crdt_state['PNCounter'][item][clientId]["dec"] = max(crdt_state['PNCounter'][item][clientId]["dec"],self.list['crdt_states']['PNCounter'][item][clientId]["dec"])
                        self.list['crdt_states']['PNCounter'][item][clientId]["inc"] = crdt_state['PNCounter'][item][clientId]["inc"]
                        self.list['crdt_states']['PNCounter'][item][clientId]["dec"] = crdt_state['PNCounter'][item][clientId]["dec"]
                        current_sum_inc += crdt_state['PNCounter'][item][clientId]["inc"]
                        current_sum_dec += crdt_state['PNCounter'][item][clientId]["dec"]
            
                    merged_value = current_sum_inc - current_sum_dec
                self.list["items"][item] = merged_value
        print(f"The self list final are before return {self.list}")
        return self.list
    

    def print_list(self): 
        print(self.list)

     