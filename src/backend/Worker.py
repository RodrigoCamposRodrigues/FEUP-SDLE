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
    with open(json_file, 'r') as file:
        data = json.load(file)
    shopping_lists = data["lists"]
    shopping_list = {}
    for shopping_list in shopping_lists:
        if int(shopping_list['id']) == int(id): 
            return shopping_list
        


def orMapToJson(orMaps, shopping_list): 
    temp = {}
    temp = orMaps[shopping_list["id"]]
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

            action = request_data.get("action")
            clientList = request_data.get("list", {})
            clientCrdtStates = clientList.get("crdt_states", {})
            pnCounterStates = clientCrdtStates.get("PNCounter", {})
            orMapStates = clientCrdtStates.get("ORMap", {})
            if orMapStates != {}: 
                orMapStates = ORMap.from_dict(orMapStates,clientId)
            if pnCounterStates != {}:
                pnCounterStates = PNCounter.from_dict(pnCounterStates)
            
            if action != "get_list": 
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
                current_listId = request_data.get("list_id")
                clientList = read_list(ident, current_listId)
                response = {"status": "success", "list": clientList}
            elif action == "update_list":
                
                data = read_file(ident)

                # Merge the existing list with the received list from the client (request)
                current_list["crdt_states"]["PNCounter"], current_list["items"] = current_list["crdt_states"]["PNCounter"].merge_version(copy.deepcopy(current_list), pnCounterStates)
                current_list["crdt_states"]["ORMap"], current_list["items"] = current_list["crdt_states"]["ORMap"].join(copy.deepcopy(current_list), orMapStates)
                current_list["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"].to_dict()
                current_list["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"].to_dict()
                for cart in data["lists"]: 
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
                data = read_file(ident)
                clientORMapListData = request_data.get("ORMapListData")

                current_list["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"].to_dict()
                current_list["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"].to_dict()

                orMapObject = ORMap.from_dict(data["ORMapList"],clientId)
                orMapObjectClient = ORMap.from_dict(clientORMapListData,clientId)
                orMapObject = orMapObject.join_lists(orMapObjectClient)
                orMapDict = orMapObject.to_dict()
                data["ORMapList"] = orMapDict   
                data["lists"].append(current_list)
                write_file(ident,data)
                print(f"Sending response to the load balancer {current_list}")
                response = {"status" :"success", "message": f"List created: {current_list}", "ORMapListData": orMapDict}   
            
            elif action =="delete_list": 
                data = read_file(ident)
                listToRemove = current_list
                clientORMapListData = request_data.get("ORMapListData")
                orMapObject = ORMap.from_dict(data["ORMapList"],clientId)
                orMapObjectClient = ORMap.from_dict(clientORMapListData,clientId)
                orMapObject = orMapObject.join_lists(orMapObjectClient)
                orMapDict = orMapObject.to_dict()
                data["ORMapList"] = orMapDict
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