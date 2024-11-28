from __future__ import print_function
import zmq
import json
import sys

# Get the shopping list from the local_list.json file 
def read_list(): 
    with open('local_list.json', 'r') as file:
        shopping_lists = json.load(file)
    return shopping_lists

def client_task(ident):
    socket = zmq.Context().socket(zmq.REQ) 
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")

    # Client requests a specific list
    shopping_lists = read_list()

    shopping_lists['items']["burger"] = 200 
    print(f"Client-{ident} updated shopping list: {shopping_lists}")
    
    # Send updated list to the load balancer
    request = {"action": "update_list", "list_id": shopping_lists['id'], "list": shopping_lists}

    # Send the request to the load balancer (ROUTER)
    socket.send(json.dumps(request).encode("utf-8"))

    # Get reply from load balancer (response from worker)
    reply = socket.recv()
    print("{}: {}".format(socket.identity.decode("ascii"),
                          reply.decode("utf-8")))

if __name__ == '__main__':
    ident = sys.argv[1] 
    client_task(ident)
