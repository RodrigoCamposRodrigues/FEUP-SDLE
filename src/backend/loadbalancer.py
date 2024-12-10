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

        print("Polling...")

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

            # Workers list updated
            if workers != previous_workers:
                print("Worker list updated. Sending updates...")
                previous_workers = workers.copy()
                worker_list_message = json.dumps({"action": "update_workers", "workers": list(workers)})
                for worker in workers:
                    backend.send_multipart([worker.encode("utf-8"), b"", b"", b"", worker_list_message.encode("utf-8")])
                    print(f"Sent worker list to worker: {worker}")

        # Handle client requests on the frontend
        if frontend in sockets:
            client, empty, request = frontend.recv_multipart()
            print("Received request from client: ", request)
            request_data = json.loads(request.decode("utf-8"))
            list_id = request_data.get("list_id", "default_key")
            assigned_workers = ring.get_preference_list(str(list_id))

            for assigned_worker in assigned_workers:
                print(f"Assigned worker: {assigned_worker} for list ID: {list_id}")
                backend.send_multipart([assigned_worker.encode("utf-8"), b"", client, b"", request])

            if not workers:
                poller.unregister(frontend)
                backend_ready = False

if __name__ == '__main__':
    main()
