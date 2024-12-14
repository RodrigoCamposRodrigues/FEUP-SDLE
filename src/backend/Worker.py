from __future__ import print_function
import zmq
import json
import sys
from GlobalCounter import GlobalCounter
from PNCounter import PNCounter
from ORMap import ORMap, DotContext
import copy

orMaps = {}
global_counter_list = {}

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
            #crdt_states = request_data.get('crdt_states', list.get('crdt_states', {}))
            print(f"The list is {clientList}")
            clientCrdtStates = clientList.get('crdt_states', {})
            pnCounterStates = clientCrdtStates.get('PNCounter', {})
            print(f"The pnCounterStates are {pnCounterStates}")
            orMapStates = clientCrdtStates.get('ORMap', {})
            print(f"The ormapsStates are {orMapStates}")
            if orMapStates != {}: 
                orMapStates = ORMap.from_dict(orMapStates,clientId)
                print(f"The ormapStates111 are {orMapStates}")
            if pnCounterStates != {}:
                pnCounterStates = PNCounter.from_dict(pnCounterStates)
                print(f"The pncounterstates are {pnCounterStates}")
            
            if action != "get_list": 
                current_list = read_list(clientList['id'],ident)
                if current_list != None: 
                    current_list['crdt_states']['ORMap'] = ORMap.from_dict(current_list['crdt_states']['ORMap'], clientId)
                    current_list['crdt_states']['PNCounter'] = PNCounter.from_dict(current_list['crdt_states']['PNCounter'])
                else : 
                    current_list = copy.deepcopy(clientList)
                    current_list['crdt_states']['ORMap'] = orMapStates
                    current_list['crdt_states']['PNCounter'] = pnCounterStates
            
            
            if action == "get_list":
                # Load the list from the json file
                print(1)
                current_listId = request_data.get("list_id")
                print(f"The current list id is {current_listId}")
                with open(f"server/server_list_{ident}.json", "r") as file:
                    print(2)
                    lists = json.load(file)
                print(3)
                for list_aux in lists:
                    print(4)
                    if int(list_aux['id']) == int(request_data.get("list_id")):
                        print(5)
                        clientList = list_aux
                        break
                print(f"The final list to be sent is {clientList}")
                response = {"status": "success", "list": clientList}
            elif action == "update_list":
                
                with open(f"server/server_list_{ident}.json", "r") as file: 
                    lists = json.load(file)

                check_lists_in_global_counter(ident)
                # Merge the existing list with the received list from the client (request)
                print(f"The action inside is {action}")
                print(f"The list inside is {clientList}")
                current_list['crdt_states']['PNCounter'], current_list['items'] = current_list['crdt_states']['PNCounter'].merge_version(copy.deepcopy(current_list), pnCounterStates)
                print(f"The new updated list PNCounter is {current_list['crdt_states']['PNCounter'].obj} with items {current_list['items']}")
                current_list['crdt_states']['ORMap'], current_list['items'] = current_list['crdt_states']['ORMap'].join(copy.deepcopy(current_list), orMapStates)
                print(f"The new updated list ORMap is {current_list['crdt_states']['ORMap'].obj}")
                print(f"The current list final ORMAPS is {current_list['crdt_states']['ORMap'].obj}")
                print(f"The current list final PNCOUNTER is {current_list['crdt_states']['PNCounter'].obj}")
                current_list['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap'].to_dict()
                current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter'].to_dict()
                for cart in lists: 
                    if cart['id'] == current_list['id']:
                        cart['items'] = current_list['items']
                        cart['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap']
                        cart['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter']

                # print(f"Updating list: {new_list['items']}")
                with open(f"server/server_list_{ident}.json", "w") as file:
                     json.dump(lists, file, indent=4)

                print(f"Sending response to the load balancer {current_list}")
                
                response = {"status": "success", "list": current_list}

                #temp = orMapToJson(orMaps, list)

                #print(f"Sending temp to the client: {temp}")
            elif action == "create_list": 
                print(f"Entering create_list")
                with open(f"server/server_list_{ident}.json", "r") as file:
                    existing_lists = json.load(file)
                
                if isinstance(existing_lists, dict): 
                    existing_lists  =json.load(file)

                print(f"The current list crdt states ORMAP are {current_list}")
                current_list['crdt_states']['ORMap'] = current_list['crdt_states']['ORMap'].to_dict()
                current_list['crdt_states']['PNCounter'] = current_list['crdt_states']['PNCounter'].to_dict()
                print(f"Sending response to the load balancer {current_list}")
                
                existing_lists.append(current_list)
                with open(f"server/server_list_{ident}.json", "w") as file:
                    json.dump(existing_lists, file, indent=4)
                response = {"status" :"success", "message": f"List created: {current_list}"}   
            
            elif action =="delete_list": 
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