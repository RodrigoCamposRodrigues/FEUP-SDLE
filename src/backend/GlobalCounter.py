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
        print(f"GlobalCounter initialized with list: {self.crdt_states}")

    def to_dict(self):
        return {"id": self.id, "list": self.list, "crdt_states" : self.crdt_states}


    def add_new_item_quantity(self, item, quantity): 
        # Initialize the array with the item and quantity
        self.list[item] = quantity
        
    def get_shopping_list(self): 
        return self
    
    def increment_value(self,id, item):
        
        #if item in self.list["items"]:
        #    self.list["items"][item] += 1
        #    self.crdt_state[item]["incs"] += 1
        #else:
        #    raise KeyError(f"Item '{item}' not found in the shopping list.")
        if item in self.crdt_states: 
            print(f"Item {item} found in the list")
            print(f"Self crdt states {self.crdt_states[item]}")
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
        
        print(f"Item {item} vector clocks: {self.crdt_states}")


    def decrement_value(self,id,item):
        if item in self.crdt_states: 
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
        
        print(f"Item {item} vector clocks: {self.crdt_states}")

    def compare_version(self, version2):
        for item in self.list["items"]: 
            if item in version2.list["items"] and self.list["items"][item] > version2.list["items"][item]:
                return 1
            elif item in version2.list["items"] and self.list["items"][item] < version2.list["items"][item]:
                return -1
        return 0

    def merge_version(self, version2, crdt_state):
        #print(f"Merging version {self.list} with version {version2}")
        #for item, quantity in version2["items"].items():
            #if item in self.list["items"]:
                #print(f"Item {item} found in the list")
                #self.crdt_state[item]["incs"] = max(
                    #self.crdt_state[item]["incs"], crdt_state[item]["incs"]
                #)
                #self.crdt_state[item]["decs"] = max(
                 #   self.crdt_state[item]["decs"], crdt_state[item]["decs"]
                #)
                #print(f"Item {item} vector clocks: {self.crdt_state[item]}")
                # Update the counter value
                #self.list["items"][item] = (
                #    self.crdt_state[item]["incs"] - self.crdt_state[item]["decs"]
                #)
                #print(f"Updated item {item} to {self.list['items'][item]}")
            #else:
                # Add new items from version2
                #self.list["items"][item] = quantity
                #self.crdt_state[item]["incs"] = crdt_state[item]["incs"]
                #self.crdt_state[item]["decs"] = crdt_state[item]["decs"]
        
        # Iterate over the crdt_state in order to get all the increments of all users 
        current_sum_inc = 0
        current_sum_dec = 0
        for item in crdt_state: 
            if item not in self.crdt_states:
                self.crdt_states[item] = {}
            merged_value = 0
            if(crdt_state[item] != {}): 
                # join the keys from the crdt_state[item] and the self.crdt_state[item]
                crdt_state_item_keys = set(crdt_state.get(item, {}).keys())
                self_crdt_states_item_keys = set(self.crdt_states.get(item, {}).keys())
                all_keys = crdt_state_item_keys.union(self_crdt_states_item_keys)
                print(f"The keys are {all_keys}")
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
                    print(f"The current sums are {current_sum_inc} and {current_sum_dec}")
        
                merged_value = current_sum_inc - current_sum_dec
            self.list["items"][item] = merged_value
        print(f"The final list is {self.list}")
        return self.list
    

    def print_list(self): 
        print(self.list)

     