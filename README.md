# SDLE Second Assignment
SDLE Second Assignment of group T01G14.

Group members:

1. Luís Vieira Relvas up202108661@edu.fe.up.pt
2. Rodrigo Campos Rodrigues up202108847@edu.fe.up.pt
3. Wallen Marcos Ribeiro up202109260@edu.fe.up.pt

## Project Grade
- 19.22/20

## Final Grade
- 17/20

## Project Description
### Shopping Lists on the Cloud
This project is a **Local-First Shopping List Application** designed as part of a **Large-Scale Distributed Systems** course. The application enables users to:
- **Create, edit, and delete shopping lists**
- **Add, remove, and modify items within lists**
- **Share lists for collaborative shopping**

### System Architecture
The system follows a **Load Balancing Broker** pattern:
- Clients and servers communicate through a **REQ-ROUTER** pattern.
- The **frontend (ROUTER)** interacts with clients, while the **backend (ROUTER)** communicates with worker nodes.

### Components
#### Client
- Maintains a **local replica** of shopping lists in JSON format.
- Provides **offline-first functionality**.
- Routes requests through a **generic load balancer**, abstracting away system complexity.

#### Load Balancer
- Implements **consistent hashing** via a **HashRing class** to distribute requests evenly across worker nodes.
- Uses **virtual nodes** to ensure balanced traffic distribution.
- Monitors worker health through a **PING mechanism**, dynamically reallocating requests if failures occur.
- Routes client requests via a **ROUTER socket** and assigns them to designated workers.

#### Worker/Servers
- Receive shopping list data and **manage replication** via preference lists.
- Perform synchronization and handle **data consistency** through **CRDT (Conflict-Free Replicated Data Types)**.

### Conflict Resolution & Fault Tolerance
- Uses **CRDTs** (PNCounter & ORMap) to merge states and resolve conflicts.
- Ensures **eventual consistency** when clients and servers synchronize.
- If a worker fails, **consistent hashing** reassigns tasks to maintain **data replication** across at least **N nodes**.

### CRDT Implementation
1. **PNCounter**: Tracks increments and decrements for items.
2. **ORMap** (for items & lists): Stores **items, tombstones**, and **dot-context** for efficient merging and deletion handling.
3. **Tombstones**: Used in ORMap to reduce computational overhead.

### Challenges Faced
- Understanding concepts like **consistent hashing, eventual consistency, and replication**.
- Implementing **conflict resolution mechanisms** efficiently.
- Coordinating team efforts to implement **server-side architecture** and **CRDT logic**.

### Future Work
- Implement a **list recovery feature** to restore deleted lists from the server.
- Introduce a **gossip protocol** for periodic **server data merging**, ensuring system-wide consistency.

---

## How to Run the Application
### Prerequisites
1. **Install Python 3**: Ensure you have Python 3 installed on your system. You can download it from [python.org](https://www.python.org/downloads/).

### Steps to Run the Application
1. **Navigate to the Backend Directory**:
    ```sh
    cd src/backend
    ```
2. **Run the Load Balancer**:
    ```sh
    python3 loadbalancer.py
    ```
3. **Run Workers**: Start the workers. Replace `[n]` with the worker ID (e.g., 0, 1, 2, ...).
    ```sh
    python3 Worker.py [n]
    ```
    Example:
    ```sh
    python3 Worker.py 0
    python3 Worker.py 1
    ```
4. **Run Clients**: Start the clients. Replace `[n]` with the client ID (e.g., 0, 1, 2, ...).
    ```sh
    python3 Client.py [n]
    ```
    Example:
    ```sh
    python3 Client.py 0
    python3 Client.py 1
    ```

### Example Workflow
1. **Start the Load Balancer**:
    ```sh
    python3 loadbalancer.py
    ```
2. **Start Three Workers**:
    ```sh
    python3 Worker.py 0
    python3 Worker.py 1
    python3 Worker.py 2
    ```
3. **Start One Client**:
    ```sh
    python3 Client.py 0
    ```

### Additional Information
- **Configuration**: Ensure that the database files (e.g., `server_list_*.json`, `local_list_*.json`) are correctly set up in the appropriate directories.
- **Troubleshooting**: If you encounter any issues, check the logs for error messages and ensure that all components are running and accessible.