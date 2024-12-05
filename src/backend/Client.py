from __future__ import print_function
import zmq
import json
import sys

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
        item_quantity = int(input(f"Enter the quantity of {item_name}: "))
        shopping_list["items"][item_name] = item_quantity
    

    json_file = 'local_list_' + ident + ".json"
    with open(json_file, 'r') as file:
        existing_data = json.load(file)

    if isinstance(existing_data, dict):
        existing_data = [existing_data]

    shopping_list["id"] = len(existing_data) + 1
    
    existing_data.append(shopping_list)
    
    with open(json_file, 'w') as file:
        json.dump(existing_data, file, indent=4)
    
    print("Shopping list created successfully!")
    return shopping_list

def client_update_list(ident):
    socket = zmq.Context().socket(zmq.REQ) 
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")

    # Ask for a specific id 
    list_id_input = input("Please enter the id of the list you want to update :")
    # Client requests a specific list
    shopping_list = read_list(ident, int(list_id_input))

    # Make a request to the loadbalancer to get the list
    if(shopping_list == None): 
        print("List not found, requesting information to the load balancer")
        request = {"action": "get_list", "list_id": list_id_input}
        socket.send(json.dumps(request).encode("utf-8"))
        reply = socket.recv()
        shopping_list = json.loads(reply.decode("utf-8"))
        shopping_list = shopping_list['list']

    # List all the items that are in the shopping list 
    print(f"Client-{ident} shopping list: {shopping_list}")

    # Ask the user to update the quantity of an item
    item_name = input("Enter the name of the item you want to update: ")
    item_quantity = int(input("Enter the new quantity: "))

    # Update the quantity of the item
    shopping_list['items'][item_name] = item_quantity

    # Change the quantity of the item in the local list
    json_file = 'local_list_' + ident + ".json"
    with open(json_file, 'r') as file:
        existing_data = json.load(file)
    
    for list_aux in existing_data:
        if int(list_aux["id"]) == int(shopping_list["id"]):
            list_aux["items"] = shopping_list["items"]
            break
    
    with open(json_file, 'w') as file:
        json.dump(existing_data, file, indent=4)


    print(f"Client-{ident} updated shopping list: {shopping_list}")
    
    # Send updated list to the load balancer
    request = {"action": "update_list", "list_id": shopping_list['id'], "list": shopping_list}

    # Send the request to the load balancer (ROUTER)
    socket.send(json.dumps(request).encode("utf-8"))

    # Get reply from load balancer (response from worker)
    reply = socket.recv()
    print("{}: {}".format(socket.identity.decode("ascii"),
                          reply.decode("utf-8")))


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

def client_create_list(ident): 
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")

    # Client requests a specific list
    shopping_lists = create_list(ident)

    # Send updated list to the load balancer
    request = {"action": "create_list", "list_id": shopping_lists['id'], "list": shopping_lists}

    # Send the request to the load balancer (ROUTER)
    socket.send(json.dumps(request).encode("utf-8"))

    # Get reply from load balancer (response from worker)
    reply = socket.recv()
    print("{}: {}".format(socket.identity.decode("ascii"),
                          reply.decode("utf-8")))


if __name__ == '__main__':
    ident = sys.argv[1] 
    client_update_list(ident)