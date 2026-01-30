# Tapdata Python SDK

A Python client library for interacting with Tapdata API.

## Features

- 🔐 Complete authentication support
- 📦 Type-safe data models
- 🎯 Clean API interface
- 🔄 Connection and task management
- 📊 Task log queries
- ⚠️ Detailed error handling
- 📝 Comprehensive documentation and type hints

## Installation

```bash
pip install tapdata-sdk
```

Or install from source:

```bash
git clone https://github.com/lddlww/tapdata_sdk.git
cd tapdata_sdk
pip install -e .
```

## Quick Start

### Basic Usage

```python
from tapdata_sdk import TapdataClient

# Initialize client
client = TapdataClient("http://localhost:3030")

# Login
client.login("admin@test.com", "password")

# Query connections
connections = client.connections.list()
for conn in connections:
    print(f"{conn.name}: {conn.status}")

# Query tasks
tasks = client.tasks.list()
for task in tasks:
    print(f"{task.name}: {task.status}")
```

### Connection Management

```python
from tapdata_sdk import ConnectionType, DatabaseType, Status

# Query source connections
source_connections = client.connections.list_source()

# Query MySQL connections
mysql_connections = client.connections.list_mysql()

# Query valid connections
valid_connections = client.connections.list_valid()

# Filter using enum types
connections = client.connections.list(
    connection_type=ConnectionType.SOURCE,
    database_type=DatabaseType.MYSQL,
    status=Status.COMPLETE
)

# Get single connection details
connection = client.connections.get("connection_id")
print(connection.endpoint)
```

### Task Management

```python
# Query running tasks
running_tasks = client.tasks.list_running()

# Query tasks with specific status
tasks = client.tasks.list(status=Status.RUNNING)

# Get task details
task = client.tasks.get("task_id")

# Start task
client.tasks.start("task_id")

# Stop task
client.tasks.stop("task_id")

# Reset task
client.tasks.reset("task_id")

# Delete task
client.tasks.delete("task_id")
```

### Query Task Logs

```python
import time

# Get logs from the last hour
end_time = int(time.time() * 1000)
start_time = end_time - 3600000  # One hour ago

logs = client.tasks.get_logs(
    task_id="task_id",
    task_record_id="record_id",
    start=start_time,
    end=end_time,
    page=1,
    page_size=20
)
```

### Error Handling

```python
from tapdata_sdk import (
    TapdataError,
    TapdataAuthError,
    TapdataTimeoutError,
    TapdataConnectionError
)

try:
    client.login("admin@test.com", "wrong_password")
except TapdataAuthError as e:
    print(f"Authentication failed: {e.message}")
except TapdataTimeoutError as e:
    print(f"Request timeout: {e.message}")
except TapdataConnectionError as e:
    print(f"Connection error: {e.message}")
except TapdataError as e:
    print(f"API error: {e.message}")
```

### Advanced Configuration

```python
# Custom timeout and SSL verification
client = TapdataClient(
    base_url="https://api.tapdata.io",
    timeout=60,  # 60 second timeout
    verify_ssl=False  # Disable SSL verification (not recommended in production)
)

# Use existing access_token
client = TapdataClient(
    base_url="http://localhost:3030",
    access_token="your-existing-token"
)

# Check authentication status
if client.is_authenticated():
    print("Authenticated")

# Logout
client.logout()
```

## API Reference

### TapdataClient

Main client class providing authentication and sub-client access.

**Parameters:**
- `base_url` (str): API base URL
- `access_token` (str, optional): Access token
- `timeout` (int): Request timeout in seconds, default 30
- `verify_ssl` (bool): Whether to verify SSL certificate, default True

**Methods:**
- `login(email, password, secret)`: User login
- `logout()`: Logout
- `is_authenticated()`: Check if authenticated
- `get_timestamp()`: Get server timestamp

**Properties:**
- `connections`: ConnectionClient instance
- `tasks`: TaskClient instance

### ConnectionClient

Connection management client.

**Methods:**
- `list(connection_type, database_type, status, skip, limit)`: Query connection list
- `get(connection_id)`: Get single connection
- `list_source()`: Get all source connections
- `list_target()`: Get all target connections
- `list_mysql()`: Get all MySQL connections
- `list_clickhouse()`: Get all ClickHouse connections
- `list_mongodb()`: Get all MongoDB connections
- `list_valid()`: Get all valid connections
- `list_invalid()`: Get all invalid connections

### TaskClient

Task management client.

**Methods:**
- `list(status, skip, limit)`: Query task list
- `get(task_id)`: Get single task
- `list_running()`: Get all running tasks
- `start(task_id)`: Start task
- `stop(task_id)`: Stop task
- `reset(task_id)`: Reset task
- `delete(task_id)`: Delete task
- `get_logs(task_id, task_record_id, start, end, page, page_size, levels)`: Get task logs

### Enum Types

```python
from tapdata_sdk import ConnectionType, DatabaseType, Status, LogLevel

# Connection types
ConnectionType.SOURCE
ConnectionType.TARGET

# Database types
DatabaseType.MYSQL
DatabaseType.CLICKHOUSE
DatabaseType.MONGODB
DatabaseType.POSTGRESQL
DatabaseType.ORACLE
DatabaseType.SQLSERVER

# Status
Status.RUNNING
Status.COMPLETE
Status.ERROR
# ... more statuses

# Log levels
LogLevel.INFO
LogLevel.WARN
LogLevel.ERROR
LogLevel.DEBUG
```

## Development

### Setup Development Environment

```bash
# Clone repository
git clone <repository-url>
cd tapdata-sdk

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black tapdata_sdk/
isort tapdata_sdk/
```

## Changelog

### v0.2.0 (2026-01-29)
- ✨ Refactored code architecture with modular design
- 📦 Added data model classes (Connection, Task, TaskLog)
- 🎯 Improved enum types using Python Enum
- 🔧 Optimized error handling with multiple exception types
- 📝 Enhanced documentation and type hints
- 🏗️ Separated client responsibilities (ConnectionClient, TaskClient)
- 🔐 Improved authentication flow
- 📊 Optimized logging

### v0.1.0
- 🎉 Initial release

## License

MIT License

## Contributing

Issues and Pull Requests are welcome!

## Support

For questions, please submit an Issue or contact the maintainers.
