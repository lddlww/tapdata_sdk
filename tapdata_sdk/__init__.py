"""
Tapdata Python SDK
A Python client library for interacting with Tapdata API.
Examples:
    >>> from tapdata_sdk import TapdataClient, ConnectionType, Status
    >>> 
    >>> # Initialize client
    >>> client = TapdataClient("http://localhost:3030")
    >>> 
    >>> # Login
    >>> client.login("admin@test.com", "password")
    >>> 
    >>> # Query connections
    >>> connections = client.connections.list(connection_type=ConnectionType.SOURCE)
    >>> 
    >>> # Query tasks
    >>> tasks = client.tasks.list_running()
    >>> 
    >>> # Operate tasks
    >>> client.tasks.stop(tasks[0].id)
"""
from .client import TapdataClient, ConnectionClient, TaskClient
from .models import Connection, Task, TaskLog, TaskDetail, TaskRelation
from .enums import ConnectionType, DatabaseType, Status, LogLevel
from .exceptions import (
    TapdataError,
    TapdataAuthError,
    TapdataConnectionError,
    TapdataValidationError,
    TapdataTimeoutError,
)

__version__ = "0.2.0"

__all__ = [
    # Client
    "TapdataClient",
    "ConnectionClient",
    "TaskClient",
    # Models
    "Connection",
    "Task",
    "TaskLog",
    # Enums
    "ConnectionType",
    "DatabaseType",
    "Status",
    "LogLevel",
    # Exceptions
    "TapdataError",
    "TapdataAuthError",
    "TapdataConnectionError",
    "TapdataValidationError",
    "TapdataTimeoutError",
]
