from collections import defaultdict
class GlobalCounter:

    shopping_lists = {} 

    def __init__(self, uuid, lista): 
        self.list = lista
        self.id = uuid
        self.shopping_lists[self.id] = self.list
        # Create a dictionary of dictionaries
        self.crdt_states = {}
        # Start the vector clocks with 0 for all the items in the list where the index 0 is A and index 1 is B and so on
        for item in self.list["items"]:
            self.crdt_states[item] = {}

    def to_dict(self):
        return {"id": self.id, "list": self.list, "crdt_states" : self.crdt_states}


    def add_new_item_quantity(self, item, quantity): 
        # Initialize the array with the item and quantity
        self.list[item] = quantity
        
    def get_shopping_list(self): 
        return self
    
    def increment_value(self,id, item):

        if item in self.list["items"]:
            if(item not in self.crdt_states): 
                self.crdt_states[item] = {} 
            if(str(id) not in self.crdt_states[item].keys()):
                print(f"Id {id} not found in the list")
                self.crdt_states[item][id] = {}
                self.crdt_states[item][id]["inc"] = 1
                self.crdt_states[item][id]["dec"] = 0
                self.list["items"][item] += 1
            else: 
                self.crdt_states[item][id]["inc"] += 1
                self.list["items"][item] += 1
        else: 
            raise KeyError(f"Item '{item}' not found in the shopping list.")
        


    def decrement_value(self,id,item):
        if item in self.list["items"]:
            if item not in self.crdt_states: 
                self.crdt_states[item] = {} 
            if id not in self.crdt_states[item].keys():
                self.crdt_states[item][id] = {}
                self.crdt_states[item][id]["inc"] = 0
                self.crdt_states[item][id]["dec"] = 1
                self.list["items"][item] -= 1
           
            else: 
                self.crdt_states[item][id]["dec"] += 1
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

    def merge_version(self, version2, crdt_state):        
        for item in crdt_state:
            current_sum_inc = 0
            current_sum_dec = 0 
            if item not in self.crdt_states:
                self.crdt_states[item] = {}
            merged_value = 0
            if(crdt_state[item] != {} or self.crdt_states[item] != {}): 
                # join the keys from the crdt_state[item] and the self.crdt_state[item]
                crdt_state_item_keys = set(crdt_state.get(item, {}).keys())
                self_crdt_states_item_keys = set(self.crdt_states.get(item, {}).keys())
                all_keys = crdt_state_item_keys.union(self_crdt_states_item_keys)
                for clientId in all_keys:
                    if clientId not in self.crdt_states[item]:
                        self.crdt_states[item][clientId] = {"inc": 0, "dec": 0}
                    if clientId not in crdt_state[item]:
                        crdt_state[item][clientId] = {"inc": 0, "dec": 0}
                    # Get always the max value between the versions 
                    crdt_state[item][clientId]["inc"] = max(crdt_state[item][clientId]["inc"],self.crdt_states[item][clientId]["inc"])
                    crdt_state[item][clientId]["dec"] = max(crdt_state[item][clientId]["dec"],self.crdt_states[item][clientId]["dec"])
                    self.crdt_states[item][clientId]["inc"] = crdt_state[item][clientId]["inc"]
                    self.crdt_states[item][clientId]["dec"] = crdt_state[item][clientId]["dec"]
                    current_sum_inc += crdt_state[item][clientId]["inc"]
                    current_sum_dec += crdt_state[item][clientId]["dec"]
        
                merged_value = current_sum_inc - current_sum_dec
            self.list["items"][item] = merged_value
        return self.list, self.crdt_states
    

    def print_list(self): 
        print(self.list)

     