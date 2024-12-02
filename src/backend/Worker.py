from __future__ import print_function
import zmq
import json
import sys


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
            print("The requested data is ", request_data)
            action = request_data.get("action")
            list = request_data.get("list")

            if action == "get_list":
                # Load the list from the json file
                with open("local_list.json", "r") as file:
                    lists = json.load(file)
                for list_aux in lists:
                    if int(list_aux["id"]) == int(request_data.get("list_id")):
                        list = list_aux
                        break
                response = {"status": "success", "list": list}
            elif action == "update_list":

                with open("local_list.json", "r") as file: 
                    lists = json.load(file) 
                for list_aux in lists: 
                    if(int(list_aux["id"]) == int(list["id"])): 
                        list_aux["items"] = list["items"]
                        break

                print(f"Updating list: {list['items']}")
                
                with open("local_list.json", "w") as file:
                    json.dump(lists, file, indent=4)
                
                response = {"status": "success", "message": f"List updated to: {list['items']}"}
            elif action == "create_list": 
                with open("local_list.json", "r") as file:
                    existing_lists = json.load(file)
                
                if isinstance(existing_lists, dict): 
                    existing_lists  =json.load(file)
                
                existing_lists.append(list)
                with open("local_list.json", "w") as file:
                    json.dump(existing_lists, file, indent=4)

                response = {"status" :"success", "message": f"List created: {list['items']}"}   
            
            elif action =="delete_list": 
                with open("local_list.json", "r") as file: 
                    existing_lists = json.load(file)
                
                for list_aux in existing_lists: 
                    if int(list_aux["id"]) == int(list["id"]): 
                        print(f"Deleting list: {list['id']}")
                        existing_lists.remove(list_aux)
                        break
            
                with open("local_list.json", "w") as file:
                    json.dump(existing_lists, file, indent=4)

                response = {"status": "success", "message": f"List deleted: {list['id']}"}
            else:
                response = {"status": "error", "message": "Invalid action"}
        except Exception as e:
            response = {"status": "error", "message": f"Failed to process request: {e}"}

        # Send response back to the backend
        socket.send_multipart([address, b"", json.dumps(response).encode("utf-8")])


if __name__ == '__main__':
    ident = sys.argv[1] 
    worker_task(ident)