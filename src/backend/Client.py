from __future__ import print_function
import zmq
import json
import sys
from GlobalCounter import GlobalCounter
import uuid
from backend.ORMap import ORMap, DotContext
from collections import defaultdict


global_counter_list = {}
orMaps = {}


def check_lists_in_global_counter(ident):
    file = "client/local_list_" + ident + ".json"
    with open(file,'r+') as file:
        shopping_lists = json.load(file)

    for shopping_list in shopping_lists: 
        if shopping_list["id"] not in global_counter_list: 
            global_counter_list[shopping_list["id"]] = GlobalCounter(shopping_list["id"], shopping_list)
            existing_data = read_list(ident, shopping_list["id"])
            global_counter_list[shopping_list["id"]].list["items"] = existing_data["items"]
            global_counter_list[shopping_list["id"]].list["crdt_states"] = existing_data["crdt_states"]


# Get the shopping list from the local_list.json file 
def read_list(ident, id):
    json_file = 'client/local_list_'+ ident + ".json" 
    with open(json_file, 'r') as file:
        shopping_lists = json.load(file)
    
    shopping_list = {}
    for shopping_list in shopping_lists:
        if shopping_list['id'] == id: 
            return shopping_list
    
def create_list(ident): 
    shopping_list = {"id": None, "name": "", "items": {}}
    print(f"------------------------------------------------------")
    shopping_list["name"] = input("Enter the name of the list: ")
    num_items = int(input("Enter the number of items in the list: "))
    map1 = ORMap(ident)
    for i in range(num_items): 
        item_name = input(f"Enter the name of item {i + 1}: ")
        shopping_list["items"][item_name] = 0
        shopping_list["crdt_states"] = {}
        shopping_list["crdt_states"]["PNCounter"] = {}
        shopping_list["crdt_states"]["PNCounter"][item_name] = {}
        shopping_list["crdt_states"]["ORMap"] = {}  
        map1.add_item(item_name)
    
    
    shopping_list["crdt_states"]["ORMap"] = map1.to_dict()
    json_file = 'client/local_list_' + ident + ".json"
    
    with open(json_file, 'r') as file:
        existing_data = json.load(file)
        
    if isinstance(existing_data, dict):
        existing_data = [existing_data]

    shopping_list["id"] = uuid.uuid4().int

    orMaps[shopping_list["id"]] = {}
    orMaps[shopping_list["id"]] = map1

    print(f"THe map1 after creating the list is {orMaps}")
    
    existing_data.append(shopping_list)
    
    with open(json_file, 'w') as file:
        json.dump(existing_data, file, indent=4)

    print("Shopping list created successfully!")
    print(f"------------------------------------------------------")
    return shopping_list


def read_file(ident): 
    json_file = "client/local_list_" + ident + ".json"
    with open(json_file, 'r') as file:
        data = json.load(file)

    return data

def write_file(ident, data): 
    json_file = "client/local_list_" + ident + ".json"
    with open(json_file, 'w') as file:
        json.dump(data, file, indent=4)

def client_update_list(ident):
    socket = create_socket(ident)
    overview = read_file(ident) 
    print(f"------------------------------------------------------")
    print(f"Client-{ident} shopping lists: ")
    for list in overview: 
        print(f"List id: {list['id']} - List name: {list['name']}")
        print(f"Items: {list['items']}")
    print(f"------------------------------------------------------")
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
        print(f"The reply_decoded is {reply_decoded}")
        shopping_list = reply_decoded.get("list")

        existing_data = read_file(ident)

        existing_data.append(shopping_list)
        
        write_file(ident, existing_data)

        shopping_list = read_list(ident, int(list_id_input))
        check_lists_in_global_counter(ident)

    print(f"---------------------------------------------------")
    print(f"Select the action you want to do with the list {shopping_list['name']}")
    print(f"1. Add an item to the list")
    print(f"2. Remove an item from the list")
    print(f"3. Update the quantity of an item")
    print(f"---------------------------------------------------")
    action = int(input("Enter the action you want to do: "))

    if action == 1: 
        # Ask the user to add an item to the list
        item_name = input("Enter the name of the item you want to add: ")
        print(f"The shopping list before adding the item is {shopping_list}")
        if shopping_list["id"] not in orMaps: 
            orMaps[shopping_list["id"]] = ORMap(ident)
        orMapsOther = ORMap.from_dict(shopping_list["crdt_states"]["ORMap"])
        print(f"THe orMaps[shopping_list[id]] in the Client is {orMaps[shopping_list['id']]}")
        print(f"The global_counter_list in the client is {global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"]}")
        print(f"The orMapsOther in the Client is {orMapsOther}")
        teste = orMaps[shopping_list["id"]].join(orMapsOther)
        orMaps[shopping_list["id"]] = teste
        global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"] = teste
        print(f"After joining the orMaps are {teste}")
        global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"] = global_counter_list[shopping_list["id"]].add_item(item_name, shopping_list["id"],ident, orMaps[shopping_list["id"]])
        print(f"---------------------------------------------------")
        print(f"The ormaps after adding the item is {global_counter_list[shopping_list["id"]].list['crdt_states']['ORMap']}")
        print(f"---------------------------------------------------")
    if action == 2: 
        # Ask the user to remove an item from the list
        item_name = input("Enter the name of the item you want to remove: ")
        print(f"The global_counter_list[shopping_list[id]] is {global_counter_list[shopping_list["id"]].list}")
        print(f"The orMaps[shopping_list[id]] is {orMaps[shopping_list['id']]}")
        global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"] = global_counter_list[shopping_list["id"]].remove_item(item_name, shopping_list["id"], ident,orMaps[shopping_list["id"]]) 
        print(f"---------------------------------------------------")
        print(f"The orMaps after removing the item is {orMaps}")
        print(f"---------------------------------------------------")
    if action == 3: 
        # Ask the user to update the quantity of an item
        item_name = input("Enter the name of the item you want to update: ")

        times_inc = input("Enter the number of times you want to increment the item: ")
        for i in range(int(times_inc)):
            global_counter_list[shopping_list["id"]].increment_value(ident,item_name)

        times_dec = input("Enter the number of times you want to decrement the item: ")
        for i in range(int(times_dec)):
            global_counter_list[shopping_list["id"]].decrement_value(ident,item_name)
            
    # Send updated list to the load balancer
    print(f"---------------------------------------------------")
    print(f"The updated list {global_counter_list[shopping_list['id']].list['name']}")
    print(f"List : {global_counter_list[shopping_list['id']].list}")
    print(f"Items: {global_counter_list[shopping_list['id']].list['items']}")
    print(f"---------------------------------------------------")
    # temp = orMapToJson(orMaps,shopping_list)
    # global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"] = orMaps[shopping_list["id"]]
    # Copy the contents of the global_Counter_list to a new variable
    global_aux = global_counter_list[shopping_list["id"]]
    if type(global_aux.list["crdt_states"]["ORMap"]) == ORMap:
        aux = ORMap.to_dict(global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"])
        global_aux.list["crdt_states"]["PNCounter"] = global_counter_list[shopping_list["id"]].to_dict()["list"]["crdt_states"]["PNCounter"]
        global_aux.list["crdt_states"]["ORMap"] = aux
        request = {
            "action": "update_list",
            "list_id": global_counter_list[shopping_list["id"]].to_dict()["id"],
            "list": global_aux.list
        }
    else: 
        request = {
            "action": "update_list",
            "list_id": global_counter_list[shopping_list["id"]].to_dict()["id"],
            "list": global_aux.list
        }

    print(f"Client-{ident} sending request to load balancer: {request}")

    # Ask the user if he wants to send the updated list to the load balancer 
    send_list = input("Do you want to send the updated list to the load balancer? (y/n): ")
    if(send_list == "y"): 
        socket.send(json.dumps(request).encode("utf-8"))
        
        reply = socket.recv()
        reply_decoded = json.loads(reply.decode("utf-8"))
        list_server = reply_decoded.get("list")
        print(f"The list_server is {list_server}")
        if type(list_server["crdt_states"]["ORMap"]) == dict:
            otherOrMaps = {
                shopping_list["id"] : ORMap.from_dict(list_server["crdt_states"]["ORMap"])
            }
        else : 
            otherOrMaps = {
                shopping_list["id"] : list_server["crdt_states"]["ORMap"]
            }
        print(f"The reply_decoded is {list_server}")
        print(f"The otherOrMaps are {otherOrMaps}")
        # Merge the existing list with the received list from the server
        global_counter_list[shopping_list["id"]].list = global_counter_list[shopping_list["id"]].merge_version(list_server,list_server["crdt_states"],otherOrMaps[shopping_list["id"]])
        print(f"Client-{ident} updated shopping list: {global_counter_list[shopping_list['id']].list}")

    # Change the quantity of the item in the local list
    existing_data  = read_file(ident)
    
    for cart in existing_data: 
        if cart["id"] == shopping_list["id"]:
            cart["items"] = global_counter_list[shopping_list["id"]].list["items"]
            if (type(global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"]) == ORMap):
                aux = ORMap.to_dict(global_counter_list[shopping_list["id"]].list["crdt_states"]["ORMap"])
                print(f"The value of the aux is {aux} and its type is {type(aux)}")
                cart["crdt_states"]["PNCounter"] = global_counter_list[shopping_list["id"]].to_dict()["list"]["crdt_states"]["PNCounter"]
                cart["crdt_states"]["ORMap"] = aux
            else : 
                cart["crdt_states"] = global_counter_list[shopping_list["id"]].to_dict()["list"]["crdt_states"]
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
    json_file = 'client/local_list_' + ident + ".json"
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

def orMapToJson(orMaps, shopping_list): 
    temp = {}
    print(f"The ormaps in Client are {orMaps}")
    print(f"The shopping list in the Client is {shopping_list}")
    temp = orMaps[shopping_list["id"]].to_dict()
    print(f"The temp in the client is {temp}")
    return temp

def client_create_list(ident): 
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")
    # Client requests a specific list
    shopping_list = create_list(ident)

    new_list = GlobalCounter(shopping_list["id"],shopping_list)

    # Send updated list to the load balancer
    # temp = orMapToJson(orMaps,shopping_list)
    
    request = {"action": "create_list", "list_id": shopping_list['id'], "list": shopping_list}

    print(f"Client-{ident} sending request to load balancer: {request}")

    answer = input("Do you want to send the list to the load balancer? (y/n): ")

    if answer == "y":
        # Send the request to the load balancer (ROUTER)
        socket.send(json.dumps(request).encode("utf-8"))

        # Get reply from load balancer (response from worker)
        reply = socket.recv()
        print("{}: {}".format(socket.identity.decode("ascii"),
                            reply.decode("utf-8")))
    





if __name__ == '__main__':
    ident = sys.argv[1] 
    while(1): 
        check_lists_in_global_counter(ident)
        print(f"---------------------------------")
        print(f"1. Create a list")
        print(f"2. Update a list")
        print(f"3. Remove a list")
        print(f"4. Exit")
        print(f"---------------------------------")
        # Ask for a number between 1 and 4 
        input_user = int(input("Enter the action you want to do: "))
       
        if input_user == 1: 
            client_create_list(ident)
        elif input_user == 2: 
            client_update_list(ident)
        elif input_user == 3: 
            client_remove_list(ident)
        elif input_user == 4: 
            break
