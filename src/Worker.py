from __future__ import print_function
import zmq
import json

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
                response = {"status": "success", "list": list}
            elif action == "update_list":
                response = {"status": "success", "message": f"List updated to: {list['items']}"}
                # 
            else:
                response = {"status": "error", "message": "Invalid action"}
        except Exception as e:
            response = {"status": "error", "message": f"Failed to process request: {e}"}

        # Send response back to the backend
        socket.send_multipart([address, b"", json.dumps(response).encode("utf-8")])
