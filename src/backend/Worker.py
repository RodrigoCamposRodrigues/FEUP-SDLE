from __future__ import print_function
import zmq
import json
import sys
from PNCounter import PNCounter
from ORMap import ORMap, DotContext
import copy

orMaps = {}
global_counter_list = {}


def read_list(ident, id):
    json_file = 'server/server_list_'+ ident + ".json"
    print(f"The json file is{json_file}") 
    with open(json_file, 'r') as file:
        data = json.load(file)
    print(f"The data inside the read_list is {data}")
    shopping_lists = data["lists"]
    print(2)
    shopping_list = {}
    print(3)
    print(f"The shopping_lists are {shopping_lists}")
    for shopping_list in shopping_lists:
        print(f"The current shopping list id is {shopping_list["id"]} and the id is {id}")
        if int(shopping_list['id']) == int(id): 
            print(f"Returning the shopping list {shopping_list}")
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

def read_file(ident): 
    json_file = "server/server_list_" + ident + ".json"
    with open(json_file, 'r') as file:
        data = json.load(file)

    return data

def write_file(ident, data): 
    json_file = "server/server_list_" + ident + ".json"
    with open(json_file, 'w') as file:
        json.dump(data, file, indent=4)


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
            clientId = address.decode("utf-8")
            clientId = clientId.split("-")[1]
            print(f"The client id is {clientId}")

            action = request_data.get("action")
            clientList = request_data.get("list", {})
            #crdt_states = request_data.get("crdt_states", list.get("crdt_states", {}))
            print(f"The list is {clientList}")
            clientCrdtStates = clientList.get("crdt_states", {})
            pnCounterStates = clientCrdtStates.get("PNCounter", {})
            print(f"The pnCounterStates are {pnCounterStates}")
            orMapStates = clientCrdtStates.get("ORMap", {})
            print(f"The ormapsStates are {orMapStates}")
            if orMapStates != {}: 
                orMapStates = ORMap.from_dict(orMapStates,clientId)
                print(f"The ormapStates111 are {orMapStates}")
            if pnCounterStates != {}:
                pnCounterStates = PNCounter.from_dict(pnCounterStates)
                print(f"The pncounterstates are {pnCounterStates}")
            
            if action != "get_list": 
                print(f"The clientList is {clientList["id"]}")
                current_list = read_list(ident,clientList["id"])
                if current_list != None: 
                    current_list["crdt_states"]["ORMap"] = ORMap.from_dict(current_list["crdt_states"]["ORMap"], clientId)
                    current_list["crdt_states"]["PNCounter"] = PNCounter.from_dict(current_list["crdt_states"]["PNCounter"])
                else : 
                    current_list = copy.deepcopy(clientList)
                    current_list["crdt_states"]["ORMap"] = orMapStates
                    current_list["crdt_states"]["PNCounter"] = pnCounterStates
            
            
            if action == "get_list":
                # Load the list from the json file
                print(1)
                current_listId = request_data.get("list_id")
                print(f"The current list id is {current_listId}")
                clientList = read_list(ident, current_listId)
                print(f"The final list to be sent is {clientList}")
                response = {"status": "success", "list": clientList}
            elif action == "update_list":
                
                data = read_file(ident)

                # Merge the existing list with the received list from the client (request)
                print(f"The action inside is {action}")
                print(f"The list inside is {clientList}")
                current_list["crdt_states"]["PNCounter"], current_list["items"] = current_list["crdt_states"]["PNCounter"].merge_version(copy.deepcopy(current_list), pnCounterStates)
                print(f"The new updated list PNCounter is {current_list["crdt_states"]["PNCounter"].obj} with items {current_list["items"]}")
                current_list["crdt_states"]["ORMap"], current_list["items"] = current_list["crdt_states"]["ORMap"].join(copy.deepcopy(current_list), orMapStates)
                print(f"The new updated list ORMap is {current_list["crdt_states"]["ORMap"].obj}")
                print(f"The current list final ORMAPS is {current_list["crdt_states"]["ORMap"].obj}")
                print(f"The current list final PNCOUNTER is {current_list["crdt_states"]["PNCounter"].obj}")
                current_list["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"].to_dict()
                current_list["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"].to_dict()
                for cart in data["lists"]: 
                    print(f"The current cart is {cart}")
                    if cart["id"] == current_list["id"]:
                        cart["items"] = current_list["items"]
                        cart["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"]
                        cart["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"]

                write_file(ident,data)

                print(f"Sending response to the load balancer {current_list}")
                
                response = {"status": "success", "list": current_list}

                #temp = orMapToJson(orMaps, list)

                #print(f"Sending temp to the client: {temp}")
            elif action == "create_list": 
                print(f"Entering create_list")
                data = read_file(ident)
                clientORMapListData = request_data.get("ORMapListData")

                print(f"The current list crdt states ORMAP are {current_list}")
                current_list["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"].to_dict()
                current_list["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"].to_dict()

                print(f"The data is {data}")
                orMapObject = ORMap.from_dict(data["ORMapList"],clientId)
                orMapObjectClient = ORMap.from_dict(clientORMapListData,clientId)
                print(f"The orMapObject is {orMapObject.obj}")
                orMapObject = orMapObject.join_lists(orMapObjectClient)
                orMapDict = orMapObject.to_dict()
                data["ORMapList"] = orMapDict   
                data["lists"].append(current_list)
                print(f"The final data is {data}")
                write_file(ident,data)
                print(f"Sending response to the load balancer {current_list}")
                response = {"status" :"success", "message": f"List created: {current_list}", "ORMapListData": orMapDict}   
            
            elif action =="delete_list": 
                data = read_file(ident)
                listToRemove = current_list
                clientORMapListData = request_data.get("ORMapListData")
                print(f"The list to remove is {listToRemove} and the clientORMapListData is {clientORMapListData}")                
                orMapObject = ORMap.from_dict(data["ORMapList"],clientId)
                print(f"The orMapObject is {orMapObject.obj}")
                orMapObjectClient = ORMap.from_dict(clientORMapListData,clientId)
                print(f"The orMapObjectClient is {orMapObjectClient.obj}")
                orMapObject = orMapObject.join_lists(orMapObjectClient)
                print(f"The orMapObject after joining is {orMapObject.obj}")
                orMapDict = orMapObject.to_dict()
                print(f"The orMapDict is {orMapDict}")
                data["ORMapList"] = orMapDict
                print(f"The data after deleting is {data}")
                write_file(ident,data)
                print(f"Sending response to the load balancer {current_list} with data {data}")


                response = {"status": "success", "message": f"List deleted: {list['id']}", "ORMapListData": orMapDict}
            else:
                response = {"status": "error", "message": "Invalid action"}
        except Exception as e:
            response = {"status": "error", "message": f"Failed to process request: {e} on action {action}"}

        # Send response back to the backend
        socket.send_multipart([address, b"", json.dumps(response).encode("utf-8")])


if __name__ == '__main__':
    ident = sys.argv[1]
    worker_task(ident)