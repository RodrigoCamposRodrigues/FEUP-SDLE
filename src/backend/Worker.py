from __future__ import print_function
import zmq
import json
import sys
from GlobalCounter import GlobalCounter

global_counter_list = {}
known_workers = set()

def check_lists_in_global_counter(ident):
    with open(f"server/server_list_{ident}.json",'r') as file:
        shopping_lists = json.load(file)

    for shopping_list in shopping_lists: 
        if shopping_list["id"] not in global_counter_list: 
            global_counter_list[shopping_list["id"]] = GlobalCounter(shopping_list["id"], shopping_list)
            existing_data = read_list(shopping_list["id"], ident)
            global_counter_list[shopping_list["id"]].list["items"] = existing_data["items"]
            global_counter_list[shopping_list["id"]].crdt_states = existing_data["crdt_states"]

def read_list(id, ident):
    json_file = f"server/server_list_{ident}.json"
    with open(json_file, 'r') as file:
        shopping_lists = json.load(file)
    
    shopping_list = {}
    for shopping_list in shopping_lists:
        if shopping_list['id'] == id: 
            return shopping_list

def worker_task(ident):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect("ipc://backend.ipc")
    known_workers = set()

    socket.send(b"READY")

    print(f"Worker-{ident} started and connected to backend.")

    while True:
        # Receive the request from the backend
        message = socket.recv_multipart()
        
        if len(message) < 3:
            print(f"Worker-{ident} received an unexpected message format: {message}")
            continue
        
        address, empty, request = message
        print(f"Worker-{ident} received request: {request.decode()}")

        # Process the request
        try:
            request_data = json.loads(request.decode("utf-8"))
            action = request_data.get("action")
            list = request_data.get("list")
            crdt_states = request_data.get("crdt_states")

            if action == "update_workers":
                new_workers = set(request_data.get("workers", []))
                if new_workers != known_workers:
                    known_workers = new_workers
                    print(f"Worker-{ident} knows the workers: {known_workers}")
                response = {"status": "success"}

            elif action == "get_list":
                with open(f"server/server_list_{ident}.json", "r") as file:
                    lists = json.load(file)
                for list_aux in lists:
                    if int(list_aux["id"]) == int(request_data.get("list_id")):
                        list = list_aux
                        break
                response = {"status": "success", "list": list}
            elif action == "update_list":
                with open(f"server/server_list_{ident}.json", "r") as file: 
                    lists = json.load(file)

                check_lists_in_global_counter(ident)
                global_counter_list[list["id"]].list, global_counter_list[list["id"]].crdt_states = global_counter_list[list["id"]].merge_version(list, crdt_states)
                print(f"The new updated list is {global_counter_list[list['id']].list}")
                for cart in lists: 
                    if int(cart["id"]) == int(list["id"]):
                        print(f"Found")
                        cart["items"] = global_counter_list[list["id"]].list["items"]
                        cart["crdt_states"] = global_counter_list[list["id"]].crdt_states
                        break
                print(f"Updating list: {global_counter_list[list['id']].list['items']}")
                with open(f"server/server_list_{ident}.json", "w") as file:
                    json.dump(lists, file, indent=4)
                
                global_counter_dict = global_counter_list[list["id"]].to_dict()
                response = {"status": "success", "list": global_counter_dict["list"], "crdt_states": global_counter_dict["crdt_states"]}
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