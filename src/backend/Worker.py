from __future__ import print_function
import zmq
import json
import sys
from GlobalCounter import GlobalCounter
from backend.ORMap import ORMap, DotContext

orMaps = {}
global_counter_list = {}

def check_lists_in_global_counter(ident):
    with open(f"server/server_list_{ident}.json",'r') as file:
        shopping_lists = json.load(file)

    for shopping_list in shopping_lists: 
        if shopping_list["id"] not in global_counter_list: 
            global_counter_list[shopping_list["id"]] = GlobalCounter(shopping_list["id"], shopping_list)
            existing_data = read_list(shopping_list["id"], ident)
            global_counter_list[shopping_list["id"]].list["items"] = existing_data["items"]
            global_counter_list[shopping_list["id"]].list["crdt_states"] = existing_data["crdt_states"]


def read_list(id, ident):
    json_file = f"server/server_list_{ident}.json"
    with open(json_file, 'r') as file:
        shopping_lists = json.load(file)
    
    shopping_list = {}
    for shopping_list in shopping_lists:
        if shopping_list['id'] == id: 
            return shopping_list
        


def orMapToJson(orMaps, shopping_list): 
    temp = {}
    print(1)
    print(f"The orMaps are {orMaps}")
    print(f"The list is {shopping_list["id"]}")
    print(f"The ormaps in the function are {orMaps[shopping_list["id"]]}")
    temp = orMaps[shopping_list["id"]]
    print(2)
    return temp


def worker_task(ident):
    context = zmq.Context()
    socket = context.socket(zmq.REQ) 
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect("ipc://backend.ipc")

    socket.send(b"READY")

    print(f"Worker-{ident} started and connected to backend.")

    while True:
        # Receive the request from the backend
        address, empty, request = socket.recv_multipart()
        print(f"Worker-{ident} received request: {request.decode()}")


        #request
        try:
            request_data = json.loads(request.decode("utf-8"))
            action = request_data.get("action")
            list = request_data.get("list", {})
            #crdt_states = request_data.get("crdt_states", list.get("crdt_states", {}))
            crdt_states = list.get("crdt_states", {})
            pn_counter_states = crdt_states.get("PNCounter", {})
            orMapsOther = ORMap.from_dict(crdt_states.get("ORMap", {}))


            if action == "get_list":
                # Load the list from the json file
                print(1)
                with open(f"server/server_list_{ident}.json", "r") as file:
                    print(2)
                    lists = json.load(file)
                print(3)
                for list_aux in lists:
                    print(4)
                    if int(list_aux["id"]) == int(request_data.get("list_id")):
                        print(5)
                        list = list_aux
                        break
                response = {"status": "success", "list": list}
            elif action == "update_list":
                with open(f"server/server_list_{ident}.json", "r") as file: 
                    lists = json.load(file)

                check_lists_in_global_counter(ident)
                # Merge the existing list with the received list from the client (request)
                print(f"The action inside is {action}")
                print(f"The list inside is {list}")
                print(f"The crdt_states inside are {crdt_states}")
                print(f"The orMapsOther inside are {orMapsOther}")
                global_counter_list[list["id"]].list = global_counter_list[list["id"]].merge_version(list, crdt_states, orMapsOther)
                print(f"The new updated list is {global_counter_list[list['id']].list}")
                for cart in lists: 
                    if int(cart["id"]) == int(list["id"]):
                        print(f"Found")
                        cart["items"] = global_counter_list[list["id"]].list["items"]
                        if type(global_counter_list[list["id"]].list["crdt_states"]["ORMap"]) == ORMap:
                            aux = ORMap.to_dict(global_counter_list[list["id"]].list["crdt_states"]["ORMap"])
                            print(f"The value of the aux is {aux} and its type is {type(aux)}")
                            cart["crdt_states"]["PNCounter"] = global_counter_list[list["id"]].to_dict()["list"]["crdt_states"]["PNCounter"]
                            cart["crdt_states"]["ORMap"] = aux
                        else: 
                            cart["crdt_states"] = global_counter_list[list["id"]].to_dict()["list"]["crdt_states"]
                        break
                print(f"Updating list: {global_counter_list[list['id']].list['items']}")
                # print(f"Updating list: {new_list['items']}")
                with open(f"server/server_list_{ident}.json", "w") as file:
                     json.dump(lists, file, indent=4)
                
                print(f"Before sending response of the list {type(global_counter_list[list["id"]].list["crdt_states"]["ORMap"])}")
                if (type(global_counter_list[list["id"]].list["crdt_states"]["ORMap"]) == ORMap):
                    print(1)
                    global_aux = global_counter_list[list["id"]]
                    aux = ORMap.to_dict(global_counter_list[list["id"]].list["crdt_states"]["ORMap"])
                    global_aux.list["crdt_states"]["PNCounter"] = global_counter_list[list["id"]].to_dict()["list"]["crdt_states"]["PNCounter"]
                    global_aux.list["crdt_states"]["ORMap"] = aux
                    response = {"status": "success", "list": global_aux.list}
                else: 
                    print(2)
                    global_aux = global_counter_list[list["id"]].to_dict()["list"]
                    print(global_aux)
                    response = {"status": "success", "list": global_aux}
                #temp = orMapToJson(orMaps, list)

                #print(f"Sending temp to the client: {temp}")
            elif action == "create_list": 
                with open(f"server/server_list_{ident}.json", "r") as file:
                    existing_lists = json.load(file)
                
                if isinstance(existing_lists, dict): 
                    existing_lists  =json.load(file)
                
                existing_lists.append(list)
                with open(f"server/server_list_{ident}.json", "w") as file:
                    json.dump(existing_lists, file, indent=4)

                response = {"status" :"success", "message": f"List created: {list['items']}"}   
            
            elif action =="delete_list": 
                with open(f"server/server_list_{ident}.json", "r") as file: 
                    existing_lists = json.load(file)
                
                for list_aux in existing_lists: 
                    if int(list_aux["id"]) == int(list["id"]): 
                        print(f"Deleting list: {list['id']}")
                        existing_lists.remove(list_aux)
                        break
            
                with open(f"server/server_list_{ident}.json", "w") as file:
                    json.dump(existing_lists, file, indent=4)

                response = {"status": "success", "message": f"List deleted: {list['id']}"}
            else:
                response = {"status": "error", "message": "Invalid action"}
        except Exception as e:
            response = {"status": "error", "message": f"Failed to process request: {e} on action {action}"}

        # Send response back to the backend
        socket.send_multipart([address, b"", json.dumps(response).encode("utf-8")])


if __name__ == '__main__':
    ident = sys.argv[1]
    check_lists_in_global_counter(ident)
    worker_task(ident)