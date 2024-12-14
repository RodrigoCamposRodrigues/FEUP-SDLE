from __future__ import print_function
import zmq
import json
import sys
import threading
from GlobalCounter import GlobalCounter
from PNCounter import PNCounter
from ORMap import ORMap, DotContext
import copy

orMaps = {}
global_counter_list = {}

def replicate_to_workers(preference_list, global_counter_dict, context):
    print("preference list is ", preference_list)
    for worker in preference_list:
        print(f"Replicating to worker {worker}")
        replicate_socket = context.socket(zmq.REQ)
        replicate_socket.connect(f"ipc://{worker}.replicate.ipc")
        try:
            replicate_request = {
                "action": "replicate_list",
                "list": global_counter_dict["list"],
                "crdt_states": global_counter_dict["crdt_states"]
            }

            replicate_socket.send_json(replicate_request)
            print(f"Sent replicate_list to worker {worker}")
            replicate_socket.recv()  
            print(f"Replicated update to worker {worker}")
        except Exception as e:
            print(f"Failed to replicate to worker {worker}: {e}")
        finally:
            replicate_socket.close()


def check_lists_in_global_counter(ident):
    with open(f"server/server_list_{ident}.json",'r') as file:
        shopping_lists = json.load(file)

    for shopping_list in shopping_lists: 
        if shopping_list['id'] not in global_counter_list: 
            global_counter_list[shopping_list['id']] = GlobalCounter(shopping_list['id'], shopping_list)
            existing_data = read_list(shopping_list['id'], ident)
            global_counter_list[shopping_list['id']].list['items'] = existing_data['items']
            global_counter_list[shopping_list['id']].list['crdt_states'] = existing_data['crdt_states']


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
    print(f"The list is {shopping_list['id']}")
    print(f"The ormaps in the function are {orMaps[shopping_list['id']]}")
    temp = orMaps[shopping_list['id']]
    print(2)
    return temp

    for shopping_list in shopping_lists:
        if shopping_list['id'] == id:
            return shopping_list
        
def merge_and_update_list(ident, client_list, pn_counter_states, or_map_states):
    with open(f"server/server_list_{ident}.json", "r") as file:
        lists = json.load(file)

    check_lists_in_global_counter(ident)
    
    current_list = read_list(client_list["id"], ident)
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
    for cart in lists:
        if cart["id"] == current_list["id"]:
            cart["items"] = current_list["items"]
            cart["crdt_states"]["ORMap"] = current_list["crdt_states"]["ORMap"]
            cart["crdt_states"]["PNCounter"] = current_list["crdt_states"]["PNCounter"]

    with open(f"server/server_list_{ident}.json", "w") as file:
        json.dump(lists, file, indent=4)

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
                list = message.get("list")
                crdt_states = message.get("crdt_states")
                global_counter_dict = merge_and_update_list(ident, list, crdt_states["PNCounter"], crdt_states["ORMap"])
                response = {"status": "success", "list": global_counter_dict["list"], "crdt_states": global_counter_dict["crdt_states"]}
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
                print(f"The client id is {clientId}")

                action = request_data.get("action")
                client_list = request_data.get("list", {})
                crdt_states = request_data.get("crdt_states", {})
                print(f"The list is {client_list}")
                pn_counter_states = crdt_states.get('PNCounter', {})
                print(f"The pnCounterStates are {pn_counter_states}")
                or_map_states = crdt_states.get('ORMap', {})
                print(f"The ormapsStates are {or_map_states}")
                if or_map_states != {}: 
                    or_map_states = ORMap.from_dict(or_map_states, clientId)
                    print(f"The ormapStates111 are {or_map_states}")
                if pn_counter_states != {}:
                    pn_counter_states = PNCounter.from_dict(pn_counter_states)
                    print(f"The pncounterstates are {pn_counter_states}")
                
                if action != "get_list": 
                    current_list = read_list(client_list['id'], ident)
                    if current_list is not None: 
                        current_list['crdt_states']['ORMap'] = ORMap.from_dict(current_list['crdt_states']['ORMap'], clientId)
                        current_list['crdt_states']['PNCounter'] = PNCounter.from_dict(current_list['crdt_states']['PNCounter'])
                    else: 
                        current_list = copy.deepcopy(client_list)
                        current_list['crdt_states']['ORMap'] = or_map_states
                        current_list['crdt_states']['PNCounter'] = pn_counter_states
                
                if action == "get_list":
                    # Load the list from the json file
                    print(1)
                    current_list_id = request_data.get("list_id")
                    print(f"The current list id is {current_list_id}")
                    with open(f"server/server_list_{ident}.json", "r") as file:
                        print(2)
                        lists = json.load(file)
                    print(3)
                    for list_aux in lists:
                        print(4)
                        if int(list_aux['id']) == int(request_data.get("list_id")):
                            print(5)
                            client_list = list_aux
                            break
                    print(f"The final list to be sent is {client_list}")
                    response = {"status": "success", "list": client_list}
                elif action == "update_list":
                    preference_list = request_data.get("preference_list", [])

                    # Update the list on the current worker
                    updated_list = merge_and_update_list(ident, client_list, pn_counter_states, or_map_states)
                    response = {"status": "success", "list": updated_list}

                    replication_thread = threading.Thread(
                        target=replicate_to_workers,
                        args=(preference_list, updated_list, context),
                        daemon=True
                    )
                    replication_thread.start()
                    
                elif action == "create_list":
                    print(f"Entering create_list")
                    with open(f"server/server_list_{ident}.json", "r") as file:
                        existing_lists = json.load(file)
                    
                    if isinstance(existing_lists, dict): 
                        existing_lists  = json.load(file)

                    print(f"The current list crdt states ORMAP are {current_list}")
                    current_list['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap'].to_dict()
                    current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter'].to_dict()
                    print(f"Sending response to the load balancer {current_list}")
                    
                    existing_lists.append(current_list)
                    with open(f"server/server_list_{ident}.json", "w") as file:
                        json.dump(existing_lists, file, indent=4)
                    response = {"status": "success", "message": f"List created: {current_list}"}   
                
                elif action == "delete_list": 
                    with open(f"server/server_list_{ident}.json", "r") as file: 
                        existing_lists = json.load(file)
                    
                    for list_aux in existing_lists: 
                        if int(list_aux['id']) == int(list['id']): 
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
