from __future__ import print_function
import zmq
import json
import sys
from GlobalCounter import GlobalCounter
import uuid


global_counter_list = {}


def check_lists_in_global_counter(ident):
    file = "local_list_" + ident + ".json"
    with open(file,'r') as file:
        shopping_lists = json.load(file)

    for shopping_list in shopping_lists: 
        if shopping_list["id"] not in global_counter_list: 
            print(f"Added list {shopping_list['id']} to the global counter")
            global_counter_list[shopping_list["id"]] = GlobalCounter(shopping_list["id"], shopping_list)
            existing_data = read_list(ident, shopping_list["id"])
            print(f"Existing data is {existing_data}")
            global_counter_list[shopping_list["id"]].list["items"] = existing_data["items"]
            global_counter_list[shopping_list["id"]].crdt_states = existing_data["crdt_states"]


# Get the shopping list from the local_list.json file 
def read_list(ident, id):
    json_file = 'local_list_'+ ident + ".json" 
    with open(json_file, 'r') as file:
        shopping_lists = json.load(file)
    
    shopping_list = {}
    for shopping_list in shopping_lists:
        print(f"Shopping list: {shopping_list}") 
        if shopping_list['id'] == id: 
            return shopping_list
    
def create_list(ident): 
    shopping_list = {"id": None, "name": "", "items": {}}
    
    shopping_list["name"] = input("Enter the name of the list: ")
    num_items = int(input("Enter the number of items in the list: "))
    
    for i in range(num_items): 
        item_name = input(f"Enter the name of item {i + 1}: ")
        shopping_list["items"][item_name] = 0
        shopping_list["crdt_states"] = {}
    

    json_file = 'local_list_' + ident + ".json"
    
    with open(json_file, 'r') as file:
        existing_data = json.load(file)
    if existing_data: 
        
        if isinstance(existing_data, dict):
            existing_data = [existing_data]

        shopping_list["id"] = uuid.uuid4().int
        
        existing_data.append(shopping_list)
        
        with open(json_file, 'w') as file:
            json.dump(existing_data, file, indent=4)
    
    else: 
        shopping_list["id"] = uuid.uuid4().int

        with open(json_file, "w") as file: 
            json.dump(shopping_list, file, indent=4)
    
    print("Shopping list created successfully!")
    return shopping_list


def read_file(ident): 
    json_file = "local_list_" + ident + ".json"
    with open(json_file, 'r') as file:
        data = json.load(file)

    return data

def write_file(ident, data): 
    json_file = "local_list_" + ident + ".json"
    with open(json_file, 'w') as file:
        json.dump(data, file, indent=4)

def client_update_list(ident):
    socket = create_socket(ident)
    # Ask for a specific id 
    list_id_input = input("Please enter the id of the list you want to update :")
    # Client requests a specific list CHANGE THIS
    shopping_list = read_list(ident, int(list_id_input))

    # Make a request to the loadbalancer to get the list
    if(shopping_list == None): 
        print("List not found, requesting information to the load balancer")
        request = {"action": "get_list", "list_id": list_id_input}
        socket.send(json.dumps(request).encode("utf-8"))
        reply = socket.recv()
        reply_decoded = json.loads(reply.decode("utf-8"))
        shopping_list = reply_decoded["list"]

        existing_data = read_file(ident)

        existing_data.append(shopping_list)
        
        write_file(ident, existing_data)

        shopping_list = read_list(ident, int(list_id_input))
        check_lists_in_global_counter(ident)

    # List all the items that are in the shopping list 
    print(f"Client-{ident} shopping list: {shopping_list}")

    # Ask the user to update the quantity of an item
    item_name = input("Enter the name of the item you want to update: ")

    times_inc = input("Enter the number of times you want to increment the item: ")
    for i in range(int(times_inc)):
        global_counter_list[shopping_list["id"]].increment_value(ident,item_name)

    times_dec = input("Enter the number of times you want to decrement the item: ")
    for i in range(int(times_dec)):
        global_counter_list[shopping_list["id"]].decrement_value(ident,item_name)


    print(f"The vector clocks are {global_counter_list[shopping_list["id"]].crdt_states}")

    
        
    print(f"Client-{ident} updated shopping list: {global_counter_list[shopping_list["id"]].list}")
    # Send updated list to the load balancer
    print(f"Sending updated list to the load balancer {global_counter_list[shopping_list["id"]].to_dict()}")
    request = {"action": "update_list", "list_id": global_counter_list[shopping_list["id"]].to_dict()["id"], "list": global_counter_list[shopping_list["id"]].to_dict()["list"], "crdt_states": global_counter_list[shopping_list["id"]].to_dict()["crdt_states"]}

    # Ask the user if he wants to send the updated list to the load balancer 
    send_list = input("Do you want to send the updated list to the load balancer? (y/n): ")
    if(send_list == "y"): 
        socket.send(json.dumps(request).encode("utf-8"))
        
        reply = socket.recv()
        reply_decoded = json.loads(reply.decode("utf-8"))
        crdt_states_server = reply_decoded.get("crdt_states")
        list_server = reply_decoded.get("list")
        print(f"Client-{ident} received response from load balancer of crdt_states: {crdt_states_server} and list: {list_server}")
        # Merge the existing list with the received list from the server
        global_counter_list[shopping_list["id"]].list, global_counter_list[shopping_list["id"]].crdt_states = global_counter_list[shopping_list["id"]].merge_version(list_server, crdt_states_server)
        print(f"Client-{ident} updated shopping list: {global_counter_list[shopping_list["id"]].list} with crdt_states {global_counter_list[shopping_list["id"]].crdt_states}")

    # Change the quantity of the item in the local list
    existing_data  = read_file(ident)
    
    print(f"The global counter list is {global_counter_list[shopping_list['id']].list}")

    for cart in existing_data: 
        if cart["id"] == shopping_list["id"]:
            print(f"Found")
            cart["items"][item_name] = global_counter_list[shopping_list["id"]].list["items"][item_name]
            # ensure that the crdt_states are updated
            cart["crdt_states"] = global_counter_list[shopping_list["id"]].crdt_states
            break

    write_file(ident, existing_data)

def create_socket(ident): 
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")
    return socket

def client_remove_list(ident): 
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")

    # Ask for a specific id
    list_id_input = input("Please enter the id of the list you want to remove :")
    list_to_send = {}    
    # Read all the lists from the local file
    json_file = 'local_list_' + ident + ".json"
    with open(json_file, 'r') as file:
        existing_data = json.load(file)
    
    # Iterate over the lists until found the one to remove
    for list_aux in existing_data:
        if int(list_aux["id"]) == int(list_id_input):
            list_to_send = list_aux
            existing_data.remove(list_aux)
            break
    
    # Save without the removed list
    with open(json_file, 'w') as file:
        json.dump(existing_data, file, indent=4)

    # Send a request with the list to remove
    request = {"action": "delete_list", "list": list_to_send}
    socket.send(json.dumps(request).encode("utf-8"))

    # Get reply from load balancer (response from worker)
    reply = socket.recv()
    print("{}: {}".format(socket.identity.decode("ascii"),
                          reply.decode("utf-8")))
    
    # Remove the list from the global_counter_list
    del global_counter_list[int(list_id_input)]

def client_create_list(ident): 
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")
    # Client requests a specific list
    shopping_list = create_list(ident)

    new_list = GlobalCounter(shopping_list["id"],shopping_list)

    # Send updated list to the load balancer
    request = {"action": "create_list", "list_id": shopping_list['id'], "list": shopping_list}

    # Send the request to the load balancer (ROUTER)
    socket.send(json.dumps(request).encode("utf-8"))

    # Get reply from load balancer (response from worker)
    reply = socket.recv()
    print("{}: {}".format(socket.identity.decode("ascii"),
                          reply.decode("utf-8")))


def send_lists_to_load_balancer():
    return global_counter_list


if __name__ == '__main__':
    ident = sys.argv[1] 
    while(1): 
        check_lists_in_global_counter(ident)
        # Ask for a number between 1 and 4 
        input_user = int(input("Enter the action you want to do: "))
        if input_user == 1: 
            client_create_list(ident)
        elif input_user == 2: 
            client_update_list(ident)
        elif input_user == 3: 
            client_remove_list(ident)
