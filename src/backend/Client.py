from __future__ import print_function
import zmq
import json
import sys
from PNCounter import PNCounter
import uuid
from ORMap import ORMap, DotContext
from collections import defaultdict
import copy



client_lists = {}
orMaps = {}



def check_lists_in_global_counter(ident):
    file = "client/local_list_" + ident + ".json"
    with open(file,'r+') as file:
        data = json.load(file)
    shopping_lists = data["lists"]
    for shopping_list in shopping_lists: 
        if shopping_list["id"] not in client_lists: 
            missingShoppingList = [temp for temp in shopping_lists if temp["id"] == shopping_list["id"]][0]
            client_lists[shopping_list["id"]] = copy.deepcopy(missingShoppingList)
            client_lists[shopping_list["id"]]['crdt_states'] = {} 
            client_lists[shopping_list["id"]]['crdt_states']['ORMap'] = {}
            client_lists[shopping_list["id"]]['crdt_states']['ORMap'] = ORMap.from_dict(client_lists[shopping_list["id"]]['crdt_states']['ORMap'],ident)
            client_lists[shopping_list["id"]]['crdt_states']['PNCounter'] = {}
            client_lists[shopping_list["id"]]['crdt_states']['PNCounter'] = PNCounter.from_dict(client_lists[shopping_list["id"]]['crdt_states']['PNCounter'])
            


# Get the shopping list from the local_list.json file 
def read_list(ident, id):
    json_file = 'client/local_list_'+ ident + ".json" 
    with open(json_file, 'r') as file:
        data = json.load(file)
    shopping_lists = data["lists"]
    shopping_list = {}
    for shopping_list in shopping_lists:
        if shopping_list['id'] == id: 
            return shopping_list
        
def read_ListOrMap(ident,listId):
    data = read_file(ident) 
    orMapsList = data['ORMapList']
    for current_list in orMapsList["items"]: 
        print(f"The current list is {current_list}")
        if current_list["items"]:
            key = list(current_list["items"].keys())[0]
            if key == listId:
                return current_list
        else:
            continue
        
        
def updateListOrMap(ident,orMapChanged, listId): 
    data = read_file(ident) 
    for current_list in data['ORMapList']: 
        if current_list["items"]:
            key = list(current_list["items"].keys())[0]
            if key == listId: 
                print(f"Making Changes ...")
                current_list.update(orMapChanged)
        else:
            continue
        

    write_file(ident,data)
    
def create_list(ident): 
    shopping_list = {'id': None, "name": "", 'items': {}}
    shopping_list['id'] = uuid.uuid4().int
    print(f"------------------------------------------------------")
    shopping_list["name"] = input("Enter the name of the list: ")
    num_items = int(input("Enter the number of items in the list: "))
    while (num_items == 0): 
        print(f"Please add at least one item to the shopping list")
        num_items = int(input("Enter the number of items in the list: "))
    map1 = ORMap(ident)
    pncounter1 = PNCounter()
    for i in range(num_items): 
        item_name = input(f"Enter the name of item {i + 1}: ")
        shopping_list['items'][item_name] = 0
        shopping_list['crdt_states'] = {}
        shopping_list['crdt_states']['ORMap'] = {}  
        map1,pncounter1 = map1.add_item(item_name, pncounter1)

    shopping_list['crdt_states']['PNCounter'] = pncounter1.obj

    client_lists[shopping_list['id']] = copy.deepcopy(shopping_list) 
    
    shopping_list['crdt_states']['ORMap'] = map1.to_dict()
    shopping_list['crdt_states']['PNCounter'] = pncounter1.to_dict()


    data = read_file(ident)
    
    data["lists"].append(shopping_list)

    
    write_file(ident, data)


    print(f"The shopping_list created is {shopping_list}")
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

def check_active_lists(ident, clientLists):
    data = read_file(ident)

    # Check which lists are in the tombstones
    deleted_lists = []
    print(f"The data inside the check_active_lists is {data}")

    if (data['ORMapList'] != {}):
        for current_list in data['ORMapList']['tombstones']: 
            deleted_lists.append(int(current_list))

        print(f"The keys to remove are {deleted_lists}")
        clientLists["lists"] = [activeList for activeList in clientLists["lists"] if int(activeList["id"]) not in deleted_lists]
        data["lists"] = [activeList for activeList in data["lists"] if int(activeList["id"]) not in deleted_lists]
        write_file(ident,data)             
        print(f"The returned clietnLists are {data}")
        
    return data
            

def client_get_list(ident,socket):
    list_id_input = input("Please enter the id of the list you want to get: ")
    print("List not found, requesting information to the load balancer")
    request = {"action": "get_list", "list_id": list_id_input}
    socket.send(json.dumps(request).encode("utf-8"))
    reply = socket.recv()
    reply_decoded = json.loads(reply.decode("utf-8"))
    print(f"The reply_decoded is {reply_decoded}")
    current_list = reply_decoded.get("list")
    serverOrMap = reply_decoded.get("RequestedORMap")
    if serverOrMap == None: 
        print(f"The requested list does not exist ! ")
        return 
    data = read_file(ident)

    if data["ORMapList"] == {}:
        data["ORMapList"] = {"items": {}, "tombstones": {}, "context": {}}
    if list_id_input not in data['ORMapList']["items"]:
        data['ORMapList']["items"][list_id_input] = {}
    data['ORMapList']["items"][list_id_input] = serverOrMap
    clientId = serverOrMap[0].split(":")[0]
    if clientId not in data['ORMapList']['context']:
        data['ORMapList']['context'][clientId] = list()
    data['ORMapList']['context'][clientId] = serverOrMap

    data["lists"].append(current_list)

    write_file(ident, data)

    current_list = read_list(ident, int(list_id_input))
    check_lists_in_global_counter(ident)
     



def client_update_list(ident, socket):
    overview = read_file(ident) 
    if overview['ORMapList'] != {}: 
        overview = check_active_lists(ident,copy.deepcopy(overview))
    if overview["lists"] != []:
        print(f"the activeLists are {overview}")
        print(f"------------------------------------------------------")
        print(f"Client-{ident} shopping lists: ")
        for list_active in overview["lists"]: 
            print(f"List id: {list_active['id']} - List name: {list_active['name']}")
            print(f"Items: {list_active['items']}")
        print(f"------------------------------------------------------")
        # Ask for a specific id 
        list_id_input = input("Please enter the id of the list you want to update :")
        # Client requests a specific list CHANGE THIS
        current_list = read_list(ident, int(list_id_input))

        # Make a request to the loadbalancer to get the list
        if(current_list == None): 
            print(f"Before updating this list you need first to get all the information about it ! ")
            return

        print(f"---------------------------------------------------")
        print(f"Select the action you want to do with the list {current_list['name']}")
        print(f"1. Add an item to the list")
        print(f"2. Remove an item from the list")
        print(f"3. Update the quantity of an item")
        print(f"---------------------------------------------------")
        action = int(input("Enter the action you want to do: "))

        current_list['crdt_states']['ORMap'] = ORMap.from_dict(current_list['crdt_states']['ORMap'],ident)
        current_list['crdt_states']['PNCounter'] = PNCounter.from_dict(current_list['crdt_states']['PNCounter'])

        if action == 1: 
            # Ask the user to add an item to the list
            item_name = input("Enter the name of the item you want to add: ")
            current_list['crdt_states']['ORMap'], current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['ORMap'].add_item(item_name, current_list['crdt_states']['PNCounter'])
            current_list["items"][item_name] = 0
        if action == 2: 
            # Ask the user to remove an item from the list
            item_name = input("Enter the name of the item you want to remove: ")
            current_list['crdt_states']['ORMap'], current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['ORMap'].delete_item(item_name, current_list['crdt_states']['PNCounter'])
            del current_list["items"][item_name]
        if action == 3: 
            # Ask the user to update the quantity of an item
            item_name = input("Enter the name of the item you want to update: ")

            times_inc = input("Enter the number of times you want to increment the item: ")
            for i in range(int(times_inc)):
                current_list['crdt_states']['PNCounter'].increment_value(ident,item_name)

            times_dec = input("Enter the number of times you want to decrement the item: ")
            for i in range(int(times_dec)):
                current_list['crdt_states']['PNCounter'].decrement_value(ident,item_name)
                
        # Send updated list to the load balancer
        print(f"---------------------------------------------------")
        print(f"The updated list {current_list['name']}")
        print(f"PNCounter Object : {current_list['crdt_states']['PNCounter'].obj}") 
        print(f"ORMap Object : {current_list['crdt_states']['ORMap'].obj}")
        print(f"ORMap dots : {current_list['crdt_states']['ORMap'].obj['context'].dots}")
        print(f"Items: {current_list['items']}")
        print(f"---------------------------------------------------")

        current_list['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap'].to_dict()
        current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter'].to_dict()

        request = {
            "action": "update_list",
            "list_id": current_list["id"],
            "list": current_list
        }

        print(f"Client-{ident} sending request to load balancer: {request}")

        # Ask the user if he wants to send the updated list to the load balancer 
        send_list = input("Do you want to send the updated list to the load balancer? (y/n): ")
        if(send_list == "y"): 
            socket.send(json.dumps(request).encode("utf-8"))
            
            reply = socket.recv()
            reply_decoded = json.loads(reply.decode("utf-8"))
            list_server = reply_decoded.get("list")
            list_server['crdt_states']['ORMap'] = ORMap.from_dict(list_server['crdt_states']['ORMap'],ident)
            list_server['crdt_states']['PNCounter'] = PNCounter.from_dict(list_server['crdt_states']['PNCounter'])

            current_list['crdt_states']['ORMap'] = ORMap.from_dict(current_list['crdt_states']['ORMap'],ident)
            current_list['crdt_states']['PNCounter'] = PNCounter.from_dict(current_list['crdt_states']['PNCounter'])

            current_list['crdt_states']['PNCounter'], current_list["items"] = current_list['crdt_states']['PNCounter'].merge_version(current_list, list_server['crdt_states']['PNCounter'])
            current_list['crdt_states']['ORMap'], current_list["items"] = current_list['crdt_states']['ORMap'].join(current_list, list_server['crdt_states']['ORMap'])
            current_list['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap'].to_dict()
            current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter'].to_dict()

        else : 
            # Just calculate the merged value of the list
            current_list['crdt_states']['ORMap'] = ORMap.from_dict(current_list['crdt_states']['ORMap'],ident)
            current_list['crdt_states']['PNCounter'] = PNCounter.from_dict(current_list['crdt_states']['PNCounter'])

            current_list['crdt_states']['PNCounter'], current_list["items"] = current_list['crdt_states']['PNCounter'].merge_version(current_list, current_list['crdt_states']['PNCounter'])
            current_list['crdt_states']['ORMap'], current_list["items"] = current_list['crdt_states']['ORMap'].join(current_list, current_list['crdt_states']['ORMap'])
        
            current_list['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap'].to_dict()
            current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter'].to_dict()
            
        data  = read_file(ident)


        
        for cart in data["lists"]: 
            if cart["id"] == current_list["id"]:
                cart["items"] = current_list["items"]
                cart['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap']
                cart['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter']

        write_file(ident, data)
    else: 
        print(f"No active lists found in the client ! ") 


def client_remove_list(ident, socket): 
    # Ask for a specific id
    list_id_input = input("Please enter the id of the list you want to remove :")
    current_list = read_list(ident, int(list_id_input))

    if current_list == None: 
        print(f"Before updating this list you need first to get all the information about it ! ")
        return

    # Read all the lists from the local file
    data = read_file(ident)

    orMapToChange = data['ORMapList']

    orMapToChangeObject = ORMap.from_dict(copy.deepcopy(orMapToChange), ident)

    orMapToChangeObject = orMapToChangeObject.delete_list(list_id_input)    

    orMapToChange = orMapToChangeObject.to_dict()

    data['ORMapList'] = orMapToChange

    print(f"the data is {data}")

    # updateListOrMap(ident,orMapToChange,list_id_input)

    # Save without the removed list
    send_list = input("Do you want to send the updated list to the load balancer? (y/n): ")
    if send_list == "y":
        data = read_file(ident)
        print(f"Sending request to the load balancer with list to remove: {current_list} with data {data['ORMapList']}")
        # Send a request with the list to remove
        request = {"action": "delete_list", "list": current_list, "ORMapListData" : orMapToChange}
        socket.send(json.dumps(request).encode("utf-8"))

        # Get reply from load balancer (response from worker)
        reply = socket.recv()
        print("{}: {}".format(socket.identity.decode("ascii"),
                            reply.decode("utf-8")))
        
        reply_decoded = json.loads(reply.decode("utf-8"))


        ORMapServer = reply_decoded.get("ORMapListData")
        ORMapServerObject = ORMap.from_dict(ORMapServer,ident)
        orMapObject = ORMap.from_dict(orMapToChange,ident)
        orMapObject = orMapObject.join_lists(ORMapServerObject)
        orMapDict = orMapObject.to_dict()
        data['ORMapList'] = orMapDict
        
    write_file(ident, data)
    

def orMapToJson(orMaps, current_list): 
    temp = {}
    temp = orMaps[current_list["id"]].to_dict()
    return temp

def client_create_list(ident, socket): 
    # Client requests a specific list
    shopping_list = create_list(ident)

    # Create the ORMap object to manage the lists (add/remote) operations
    data = read_file(ident) 

    if data['ORMapList'] == {}: 
        map2 = ORMap(ident).add_list(shopping_list["id"])
        map2 = map2.to_dict()
        data['ORMapList'] = map2
    else: 
        orMapObject = ORMap.from_dict(data['ORMapList'], ident)
        orMapObject = orMapObject.add_list(shopping_list["id"])
        data['ORMapList'] = orMapObject.to_dict()

    orMapObject = ORMap.from_dict(data['ORMapList'],ident)

    write_file(ident,data)
    
    
    request = {"action": "create_list", "list_id": shopping_list['id'], "list": shopping_list, "ORMapListData" : data['ORMapList']}

    print(f"Client-{ident} sending request to load balancer: {request}")

    answer = input("Do you want to send the list to the load balancer? (y/n): ")

    if answer == "y":
        # Send the request to the load balancer (ROUTER)
        socket.send(json.dumps(request).encode("utf-8"))

        # Get reply from load balancer (response from worker)
        reply = socket.recv()
    
        reply_decoded = json.loads(reply.decode("utf-8"))   

        ORMapServer = reply_decoded.get("ORMapListData")
        ORMapServerObject = ORMap.from_dict(ORMapServer,ident)
        print(f"The ORMapServerObject is {ORMapServerObject.obj}")
        print(f"The orMapObject is {orMapObject.obj}")
        orMapObject = orMapObject.join_lists(ORMapServerObject) 
        orMapDict = orMapObject.to_dict()
        data['ORMapList'] = orMapDict
        print("{}: {}".format(socket.identity.decode("ascii"),
                            reply.decode("utf-8")))
    

if __name__ == '__main__':
    ident = sys.argv[1] 
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")
    while(1): 
        check_lists_in_global_counter(ident)
        print(f"---------------------------------")
        print(f"1. Create a list")
        print(f"2. Update a list")
        print(f"3. Remove a list")
        print(f"4. Get a list")
        print(f"5. Exit")
        print(f"---------------------------------")
        # Ask for a number between 1 and 4 
        input_user = int(input("Enter the action you want to do: "))
       
        if input_user == 1: 
            client_create_list(ident, socket)
        elif input_user == 2: 
            client_update_list(ident, socket)
        elif input_user == 3: 
            client_remove_list(ident, socket)
        elif input_user == 4: 
            client_get_list(ident,socket)
        elif input_user == 5:
            break
