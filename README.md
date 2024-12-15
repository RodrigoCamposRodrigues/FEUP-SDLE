# SDLE Second Assignment

SDLE Second Assignment of group T01G14.

Group members:

1. Luís Vieira Relvas up202108661@edu.fe.up.pt
2. Rodrigo Campos Rodrigues up202108847@edu.fe.up.pt
3. Wallen Marcos Ribeiro up202109260@edu.fe.up.pt


## How to Run the Application

### Prerequisites

1. **Install Python 3**: Ensure you have Python 3 installed on your system. You can download it from [python.org](https://www.python.org/downloads/).


### Steps to Run the Application

1. **Navigate to the Backend Directory**: Navigate to the `src/backend` directory where the application scripts are located.

    ```cd src/backend```

2. **Run the Load Balancer**: Start the load balancer.

    ```python3 loadbalancer.py```

3. **Run Workers**: Start the workers. Replace `[n]` with the worker ID (e.g., 0, 1, 2, ...).

    ```python3 Worker.py [n]```

    Example:

    ```python3 Worker.py 0```

    ```python3 Worker.py 1```

4. **Run Clients**: Start the clients. Replace `[n]` with the client ID (e.g., 0, 1, 2, ...).

    ```python3 Client.py [n]```

    Example:

    ```python3 Client.py 0```

    ```python3 Client.py 1```

### Example Workflow

1. **Start the Load Balancer**:

    ```python3 loadbalancer.py```

2. **Start Three Workers**:

    ```python3 Worker.py 0```

    ```python3 Worker.py 1```
    
    ```python3 Worker.py 2```

3. **Start One Client**:

    ```python3 Client.py 0```

### Additional Information

- **Configuration**: Ensure that the database files (e.g., `server_list_*.json`, `local_list_*.json`) are correctly set up in the appropriate directories.
- **Troubleshooting**: If you encounter any issues, check the logs for error messages and ensure that all components are running and accessible.