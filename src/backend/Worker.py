from __future__ import print_function
import zmq
import json
import sys
import threading
from PNCounter import PNCounter
from ORMap import ORMap, DotContext
import copy

orMaps = {}
global_counter_list = {}

def replicate_to_workers(preference_list, clientList, context, clientId):
    
    print("preference list is ", preference_list)
    for worker in preference_list:
        print(f"Replicating to worker {worker}")
        replicate_socket = context.socket(zmq.REQ)
        replicate_socket.connect(f"ipc://{worker}.replicate.ipc")
        try:
            replicate_request = {
                "action": "replicate_list",
                "list": clientList,
                "client_id": clientId
            }

            replicate_socket.send_json(replicate_request)
            print(f"Sent replicate_list to worker {worker}")
            replicate_socket.recv()  
            print(f"Replicated update to worker {worker}")
        except Exception as e:
            print(f"Failed to replicate to worker {worker}: {e}")
        finally:
            replicate_socket.close()

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


def merge_and_update_list(ident, client_list, pn_counter_states, or_map_states):
    data = read_file(ident)

    current_list = read_list(ident, client_list["id"])
    if current_list is not None:
        current_list["crdt_states"]["ORMap"] = ORMap.from_dict(current_list["crdt_states"]["ORMap"], client_list["id"])
        current_list["crdt_states"]["PNCounter"] = PNCounter.from_dict(current_list["crdt_states"]["PNCounter"])
    else:
        current_list = copy.deepcopy(client_list)
        current_list["crdt_states"]["ORMap"] = or_map_states
        current_list["crdt_states"]["PNCounter"] = pn_counter_states

    print(f"The action inside is update_list")
    print(f"The list inside is {client_list}")
    current_list["crdt_states"]["PNCounter"], current_list["items"] = current_list["crdt_states"]["PNCounter"].merge_version(copy.deepcopy(current_list), pn_counter_states)
    print(f"The new updated list PNCounter is {current_list['crdt_states']['PNCounter'].obj} with items {current_list['items']}")
    current_list["crdt_states"]["ORMap"], current_list["items"] = current_list["crdt_states"]["ORMap"].join(copy.deepcopy(current_list), or_map_states)
    print(f"The new updated list ORMap is {current_list['crdt_states']['ORMap'].obj}")
    print(f"The current list final ORMAPS is {current_list['crdt_states']['ORMap'].obj}")
    print(f"The current list final PNCOUNTER is {current_list['crdt_states']['PNCounter'].obj}")
    current_list["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"].to_dict()
    current_list["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"].to_dict()
    flag = False
    for cart in data["lists"]:
        if cart["id"] == current_list["id"]:
            flag = True
            cart["items"] = current_list["items"]
            cart["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"]
            cart["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"]

    if (flag == False):
        data["lists"].append(current_list)
    
    print("THE DATA IS ", data)

    write_file(ident, data)

    print(f"Sending response to the load balancer {current_list}")
    return current_list

def worker_task(ident):
    print(f"Starting Worker-{ident}")
    context = zmq.Context()

    socket = context.socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect("ipc://backend.ipc")

    health_socket = context.socket(zmq.REP)
    health_socket.bind(f"ipc://Worker-{ident}.ipc")

    replicate_socket = context.socket(zmq.REP)
    replicate_socket.bind(f"ipc://Worker-{ident}.replicate.ipc")

    socket.send(b"READY")

    print(f"Worker-{ident} started and connected to backend.")

    while True:
        print(f"Worker-{ident} polling for events...")
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        poller.register(health_socket, zmq.POLLIN)
        poller.register(replicate_socket, zmq.POLLIN)

        events = dict(poller.poll())

        print(f"Worker-{ident} events: {events}")

        if replicate_socket in events:
            message = replicate_socket.recv_json()
            print(f"Worker-{ident} received replication message: {message}")

            action = message.get("action")
            if action == "replicate_list":
                clientList = message.get("list")
                clientId = message.get("client_id")

                clientCrdtStates = clientList.get("crdt_states", {})
                pnCounterStates = clientCrdtStates.get("PNCounter", {})
                orMapStates = clientCrdtStates.get("ORMap", {})
                if orMapStates != {}: 
                    orMapStates = ORMap.from_dict(orMapStates,clientId)
                if pnCounterStates != {}:
                    pnCounterStates = PNCounter.from_dict(pnCounterStates)

                updated_list = merge_and_update_list(ident, clientList, pnCounterStates, orMapStates)
                response = {"status": "success", "list": clientList}
                replicate_socket.send_json(response)
            continue

        # Handle health-check requests
        if health_socket in events:
            message = health_socket.recv()
            print(f"Worker-{ident} received health-check message: {message}")
            if message == b"PING":
                health_socket.send(b"PONG")
            continue

        # Handle client backend requests
        if socket in events:
            message = socket.recv_multipart()
            print(f"Worker-{ident} received message: {message}")

            if len(message) < 3:
                print(f"Worker-{ident} received an unexpected message format: {message}")
                continue

            address, empty, request = message
            print(f"Worker-{ident} received request: {request.decode()}")

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
                    preference_list = request_data.get("preference_list", [])

                    # Update the list on the current worker
                    updated_list = merge_and_update_list(ident, clientList, pnCounterStates, orMapStates)
                    response = {"status": "success", "list": updated_list}


                    replication_thread = threading.Thread(
                        target=replicate_to_workers,
                        args=(preference_list, updated_list, context, clientId),
                        daemon=True
                    )
                    replication_thread.start()
                    
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
