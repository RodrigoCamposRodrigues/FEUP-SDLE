from collections import defaultdict
from ORMap import ORMap, DotContext
class PNCounter:

    shopping_lists = {} 

    def __init__(self): 
        #self.list = lista
        #self.id = uuid
        #self.shopping_lists[self.id] = self.list
        # Create a dictionary of dictionaries
        # Start the vector clocks with 0 for all the items in the list where the index 0 is A and index 1 is B and so on
        #self.list["crdt_states"] = {} 
        #self.list["crdt_states"]["PNCounter"] = {}
        #for item in self.list["items"]:
        #    self.list["crdt_states"]["PNCounter"][item] = {}
        self.obj = {}
                
    def setup_item(self, item, clientId): 
        print(f"The self is {self.obj}")
        if item not in self.obj: 
            self.obj[item] = {}
        if clientId not in self.obj[item]: 
            self.obj[item][clientId] = {"inc": 0, "dec": 0}

        print(f"Returning from the setup_item function {self.obj}")
        return self

    def to_dict(self):
        return self.obj
    
    @classmethod
    def from_dict(cls, data):
        print(f"the data is {data}")
        instance = cls()
        instance.obj = data
        return instance
    
    def add_item(self, item, listId,clientId,orMapsOther): 
        if item not in self.list["items"]:
            self.obj["items"][item] = 0
            self.obj["crdt_states"]["PNCounter"][item] = {}
            orMapsOther.add_item(item)
            if listId not in self.orMaps: 
                self.orMaps[listId] = {}
            self.orMaps[listId] = orMapsOther
        else: 
            raise KeyError(f"Item '{item}' already exists in the shopping list.")
        print(f"The self is {self.list}")
        return orMapsOther
        
    def remove_item(self, item): 
        if item in self.obj: 
            del self.obj[item]

        return self
    
    def add_new_item_quantity(self, item, quantity): 
        # Initialize the array with the item and quantity
        self.list[item] = quantity
        
    def get_shopping_list(self): 
        return self
    
    def increment_value(self,id, item):

        if item in self.obj:
            if(item not in self.obj): 
                self.obj[item] = {} 
            if(id not in self.obj[item].keys()):
                print(f"Id {id} not found in the list")
                self.obj[item][id] = {}
                self.obj[item][id]["inc"] = 1
                self.obj[item][id]["dec"] = 0

            else: 
                self.obj[item][id]["inc"] += 1
        else: 
            raise KeyError(f"Item '{item}' not found in the shopping list.")
        


    def decrement_value(self,id,item):
        if item in self.obj:
            if item not in self.obj: 
                self.obj[item] = {} 
            if id not in self.obj[item].keys():
                self.obj[item][id] = {}
                self.obj[item][id]["inc"] = 0
                self.obj[item][id]["dec"] = 1
            else: 
                self.obj[item][id]["dec"] += 1
        else:
            raise KeyError(f"Item '{item}' not found in the shopping list.")
        

    def compare_version(self, version2):
        for item in self.list["items"]: 
            if item in version2.list["items"] and self.list["items"][item] > version2.list["items"][item]:
                return 1
            elif item in version2.list["items"] and self.list["items"][item] < version2.list["items"][item]:
                return -1
        return 0

    def merge_version(self, currentList ,otherPNCounterState):

            for item in otherPNCounterState.obj:
                print(5)
                current_sum_inc = 0
                current_sum_dec = 0 
                if item not in self.obj:
                    print(6)
                    # self.crdt_states["PNCounter"][item] = {}
                    self.obj[item] = {}
                merged_value = 0
                print(7)
                if(otherPNCounterState.obj[item] != {} or self.obj[item] != {}): 
                    # join the keys from the crdt_state[item] and the self.crdt_state[item]
                    print(8)
                    other_crdt_state_item_keys = set(otherPNCounterState.obj.get(item, {}).keys())
                    self_crdt_state_item_keys = set(self.obj.get(item, {}).keys())
                    print(9)
                    all_keys = other_crdt_state_item_keys.union(self_crdt_state_item_keys)
                    print(f"The all keys are {all_keys}")
                    for clientId in all_keys:
                        print(10)
                        if item not in self.obj: 
                            print(11)
                            self.obj[item] = {}
                        if clientId not in self.obj[item]:
                            print(11.5)
                            self.obj[item][clientId] = {"inc": 0, "dec": 0}
                        print(11.6)
                        if clientId not in otherPNCounterState.obj[item]:
                            print(12)
                            otherPNCounterState.obj[item][clientId] = {"inc": 0, "dec": 0}
                        # Get always the max value between the versions
                        print(13)
                        otherPNCounterState.obj[item][clientId]["inc"] = max(otherPNCounterState.obj[item][clientId]["inc"],self.obj[item][clientId]["inc"])
                        print(14)
                        otherPNCounterState.obj[item][clientId]["dec"] = max(otherPNCounterState.obj[item][clientId]["dec"],self.obj[item][clientId]["dec"])
                        print(15)
                        self.obj[item][clientId]["inc"] = otherPNCounterState.obj[item][clientId]["inc"]
                        print(16)
                        self.obj[item][clientId]["dec"] = otherPNCounterState.obj[item][clientId]["dec"]
                        print(17)
                        current_sum_inc += otherPNCounterState.obj[item][clientId]["inc"]
                        print(19)
                        current_sum_dec += otherPNCounterState.obj[item][clientId]["dec"]
                    print(20)
                    merged_value = current_sum_inc - current_sum_dec
                print(21)
                currentList["items"][item] = merged_value
            print(22)
            print(f"The self list final are before return {currentList} with PNCounter {self.obj}")
            return self, currentList["items"]
    

    def print_list(self): 
        print(self.list)

     