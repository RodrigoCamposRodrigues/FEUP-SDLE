from __future__ import print_function
import multiprocessing
import zmq
import Worker
import json
from HashRing import HashRing


def main():
    backend_ready = False
    workers = set()  # Use a set to manage workers dynamically
    ring = HashRing(workers)
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("ipc://frontend.ipc")

    backend = context.socket(zmq.ROUTER)
    backend.bind("ipc://backend.ipc")

    # Poller for load balancer
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)  # Monitor frontend for client requests
    poller.register(backend, zmq.POLLIN)  # Monitor backend for worker responses

    print("Broker is ready and polling...")

    while True:
        sockets = dict(poller.poll(timeout=1000))
        if not sockets:
            print("No activity detected.")
            continue

        print("Polling...")

        # Handle worker responses on the backend
        if backend in sockets:
            request = backend.recv_multipart()
            print(f"Received response from worker: {request}")
            worker, empty, client = request[:3]
            workers.add(worker.decode("utf-8"))
            ring = HashRing(workers)  
            print(f"Workers: {workers}")
            if workers and not backend_ready:
                poller.register(frontend, zmq.POLLIN)
                backend_ready = True
            if client != b"READY" and len(request) > 3:
                empty, reply = request[3:]
                frontend.send_multipart([client, b"", reply])

        # Handle client requests on the frontend
        if frontend in sockets:
            client, empty, request = frontend.recv_multipart()
            request_data = json.loads(request.decode("utf-8"))
            list_id = request_data.get("list_id", "default_key")
            assigned_worker = ring.get_node(str(list_id))
            print(f"Assigned worker: {assigned_worker} for list ID: {list_id}")
            backend.send_multipart([assigned_worker.encode("utf-8"), b"", client, b"", request])
            if not workers:
                poller.unregister(frontend)
                backend_ready = False


if __name__ == '__main__':
    main()