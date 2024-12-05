from collections import defaultdict
class GlobalCounter:

    shopping_lists = {} 

    def __init__(self, uuid, lista): 
        self.list = lista
        self.id = uuid
        self.shopping_lists[self.id] = self.list
        self.vector_clocks = defaultdict(lambda: {"incs" : defaultdict(int), "decs" : defaultdict(int)})
        # Start the vector clocks with 0 for all the items in the list where the index 0 is A and index 1 is B and so on
        for item in self.list["items"]:
            self.vector_clocks[item]["incs"] = 0
            self.vector_clocks[item]["decs"] = 0
        print(f"GlobalCounter initialized with list: {self.vector_clocks}")

    def to_dict(self):
        return {"id": self.id, "list": self.list, "vector_clocks" : self.vector_clocks}


    def add_new_item_quantity(self, item, quantity): 
        # Initialize the array with the item and quantity
        self.list[item] = quantity
        
    def get_shopping_list(self): 
        return self
    
    def increment_value(self, item):
        if item in self.list["items"]:
            self.list["items"][item] += 1
            self.vector_clocks[item]["incs"] += 1
        else:
            raise KeyError(f"Item '{item}' not found in the shopping list.")


    def decrement_value(self,item):
        if item in self.list["items"]: 
            self.list["items"][item] -= 1 
            self.vector_clocks[item]["decs"] += 1  
        else: 
            raise KeyError(f"Item '{item}' not found in the shopping list.")

    def compare_version(self, version2):
        for item in self.list["items"]: 
            if item in version2.list["items"] and self.list["items"][item] > version2.list["items"][item]:
                return 1
            elif item in version2.list["items"] and self.list["items"][item] < version2.list["items"][item]:
                return -1
        return 0

    def merge_version(self, version2, vector_clocks):
        print(f"Merging version {self.list} with version {version2}")
        for item, quantity in version2["items"].items():
            if item in self.list["items"]:
                print(f"Item {item} found in the list")
                self.vector_clocks[item]["incs"] = max(
                    self.vector_clocks[item]["incs"], vector_clocks[item]["incs"]
                )
                self.vector_clocks[item]["decs"] = max(
                    self.vector_clocks[item]["decs"], vector_clocks[item]["decs"]
                )
                print(f"Item {item} vector clocks: {self.vector_clocks[item]}")
                # Update the counter value
                self.list["items"][item] = (
                    self.vector_clocks[item]["incs"] - self.vector_clocks[item]["decs"]
                )
                print(f"Updated item {item} to {self.list['items'][item]}")
            else:
                # Add new items from version2
                self.list["items"][item] = quantity
                self.vector_clocks[item]["incs"] = vector_clocks[item]["incs"]
                self.vector_clocks[item]["decs"] = vector_clocks[item]["decs"]

        return self.list
    

    def print_list(self): 
        print(self.list)

     