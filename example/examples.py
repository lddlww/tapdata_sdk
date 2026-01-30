"""
Tapdata SDK Usage Examples
"""
from tapdata_sdk import (
    TapdataClient,
    ConnectionType,
    DatabaseType,
    Status,
    TapdataError,
)


def example_basic_usage():
    """Basic usage example"""
    # Initialize client
    client = TapdataClient("http://localhost:3030")
    
    # Login
    try:
        token = client.login("admin@admin.com", "password")
        print("✓ Login successful", token)
    except TapdataError as e:
        print(f"✗ Login failed: {e.message}")
        return
    
    # Query connections
    connections = client.connections.list(name="test")
    print(f"\nTotal {len(connections)} connections:")
    for conn in connections:
        print(f"  - {conn}")
    
    # Query tasks
    tasks = client.tasks.list(name="test")
    print(f"\nTotal {len(tasks)} tasks:")
    for task in tasks:
        print(f"  - {task}")


def example_connection_management():
    """Connection management example"""
    client = TapdataClient("http://localhost:3030")
    client.login("admin@admin.com", "password")
    
    # Query source connections
    print("\n=== Source Connections ===")
    source_connections = client.connections.list_source()
    for conn in source_connections:
        print(f"  {conn}")
    
    # Query MySQL connections
    print("\n=== MySQL Connections ===")
    mysql_connections = client.connections.list_mysql()
    for conn in mysql_connections:
        print(f"  {conn.name}: {conn.status}")
    
    # Query valid connections
    print("\n=== Valid Connections ===")
    valid_connections = client.connections.list_valid()
    print(f"  Total {len(valid_connections)} valid connections")
    
    # Filter using enums
    print("\n=== MySQL Source Connections ===")
    connections = client.connections.list(
        connection_type=ConnectionType.SOURCE,
        database_type=DatabaseType.MYSQL,
        status=Status.VALID
    )
    for conn in connections:
        print(f"  {conn.name}")


def example_task_management():
    """Task management example"""
    client = TapdataClient("http://localhost:3030")
    client.login("admin@admin.com", "password")
    
    # Query running tasks
    print("\n=== Running Tasks ===")
    running_tasks = client.tasks.list_running()
    
    if running_tasks:
        task = running_tasks[0]
        print(f"Found task: {task.name} ({task.id})")
        
        # Stop task
        print("\nStopping task...")
        try:
            result = client.tasks.stop(task.id)
            print("✓ Task stopped")
            sleep(5)
            result = client.tasks.start(task.id)
            print("✓ Task started", result)
        except TapdataError as e:
            print(f"✗ Stop failed: {e.message}")
    else:
        print("No running tasks")
    
    # Query all tasks
    all_tasks = client.tasks.list()
    print(f"\nTotal {len(all_tasks)} tasks:")
    
    # Group by status
    from collections import defaultdict
    tasks_by_status = defaultdict(list)
    for task in all_tasks:
        tasks_by_status[task.status].append(task)
    
    for status, tasks in tasks_by_status.items():
        print(f"  {status}: {len(tasks)} tasks")


def example_task_logs():
    """Task log query example"""
    import time
    
    client = TapdataClient("http://localhost:3030")
    client.login("admin@admin.com", "password")
    
    # Get first task
    tasks = client.tasks.list()
    if not tasks:
        print("No available tasks")
        return
    
    task = tasks[0]
    print(f"Querying task logs: {task.name}")
    
    if not task.task_record_id:
        print("Task has no task_record_id")
        return
    
    # Query logs from the last hour
    end_time = int(time.time() * 1000)
    start_time = end_time - 50000  # 10 seconds ago
    
    try:
        logs = client.tasks.get_logs(
            task_id=task.id,
            task_record_id=task.task_record_id,
            start=1769492201601,
            end=int(time.time()*1000),
            page=1,
            page_size=20
        )
        
        print(f"\nFound {len(logs)} log entries:")
        for log in logs:  # Show only first 5
            print(f"  {log}")
    except TapdataError as e:
        print(f"✗ Query logs failed: {e.message}")


def example_error_handling():
    """Error handling example"""
    from tapdata_sdk import (
        TapdataAuthError,
        TapdataTimeoutError,
        TapdataConnectionError,
    )
    
    client = TapdataClient("http://localhost:3030")
    
    # Handle authentication error
    try:
        client.login("wrong@email.com", "wrong_password")
    except TapdataAuthError as e:
        print(f"Authentication failed: {e.message}")
        print(f"Error code: {e.code}")
    
    # Handle timeout error
    slow_client = TapdataClient(
        "http://localhost:3030",
        timeout=1  # 1 second timeout
    )
    try:
        slow_client.get_timestamp()
    except TapdataTimeoutError as e:
        print(f"Request timeout: {e.message}")
    
    # Handle connection error
    bad_client = TapdataClient("http://invalid-host:3030")
    try:
        bad_client.get_timestamp()
    except TapdataConnectionError as e:
        print(f"Connection error: {e.message}")


def example_advanced_features():
    """Advanced features example"""
    # Use existing token
    client = TapdataClient(
        base_url="http://localhost:3030",
        access_token="existing-token-here"
    )
    
    # Check authentication status
    if client.is_authenticated():
        print("✓ Authenticated")
    else:
        print("✗ Not authenticated, need to login")
    
    # Custom configuration
    custom_client = TapdataClient(
        base_url="http://localhost:3030",
        timeout=60,  # 60 second timeout
        verify_ssl=False  # Disable SSL verification (for development only)
    )
    
    # Logout when done
    client.logout()
    print("✓ Logged out")


if __name__ == "__main__":
    print("=" * 60)
    print("Tapdata SDK Examples")
    print("=" * 60)
    
    # Run examples (uncomment the one you want to run)
    # example_basic_usage()
    # example_connection_management()
    # example_task_management()
    example_task_logs()
    # example_error_handling()
    # example_advanced_features()
    
    print("\nPlease uncomment the example function you want to run")
