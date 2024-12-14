from __future__ import print_function
import threading
import zmq
import Worker
import json
from HashRing import HashRing
import time

def check_worker_health(context, workers, removed_workers):
    while True:
        for worker in list(workers):  
            try:
                health_socket = context.socket(zmq.REQ)
                health_socket.linger = 0
                health_socket.connect(f"ipc://{worker}.ipc")
                
                health_socket.send(b"PING")
                
                if not health_socket.poll(timeout=1000):  # 1-second timeout
                    raise Exception(f"Worker {worker} did not respond.")

                reply = health_socket.recv()
                if reply != b"PONG":
                    raise Exception(f"Unexpected response from {worker}: {reply}")
            
            except Exception as e:
                print(f"Worker {worker} is down: {e}")
                workers.remove(worker)  
                removed_workers.add(worker) 
                print("Workers: ", workers)


            finally:
                health_socket.close()

        time.sleep(5)  

def main():
    count = 2
    backend_ready = False
    workers = set()
    removed_workers = set()
    previous_workers = set()  
    ring = HashRing()
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("ipc://frontend.ipc")

    backend = context.socket(zmq.ROUTER)
    backend.bind("ipc://backend.ipc")

    # worker life checker thread
    health_checker = threading.Thread(
        target=check_worker_health, args=(context, workers, removed_workers), daemon=True
    )
    health_checker.start()

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)  
    poller.register(backend, zmq.POLLIN)  

    print("Broker is ready and polling...")

    while True:
        sockets = dict(poller.poll(timeout=1000))
        if not sockets:
            print("No activity detected.")
            continue

        # Handle worker responses on the backend
        if backend in sockets:
            request = backend.recv_multipart()
            print(f"Received response from worker: {request}")
            worker, empty, client = request[:3]
            workers.add(worker.decode("utf-8"))
            ring = HashRing(workers)
            print(f"Workers {workers}")

            if workers and not backend_ready:
                poller.register(frontend, zmq.POLLIN)
                backend_ready = True

            if client != b"READY" and len(request) > 3:
                empty, reply = request[3:]
                frontend.send_multipart([client, b"", reply])
       
        # Handle client requests on the frontend
        if frontend in sockets:
            client, empty, request = frontend.recv_multipart()
            print("Received request from client: ", request)
            request_data = json.loads(request.decode("utf-8"))
            list_id = request_data.get("list_id", "default_key")
            preference_list = ring.get_preference_list(str(list_id))
            coordinator_node = ring.get_node(str(list_id))

            preference_list.remove(coordinator_node)

            request_data["preference_list"] = preference_list
            
            # send client request data and 2 neighbor nodes to replicate the data
            backend.send_multipart([coordinator_node.encode("utf-8"), b"", client, b"", json.dumps(request_data).encode("utf-8")])

            if not workers:
                poller.unregister(frontend)
                backend_ready = False

if __name__ == '__main__':
    main()
