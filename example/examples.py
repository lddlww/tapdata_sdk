"""
Tapdata SDK 使用示例
"""
from tapdata_sdk import (
    TapdataClient,
    ConnectionType,
    DatabaseType,
    Status,
    TapdataError,
)


def example_basic_usage():
    """基本使用示例"""
    # 初始化客户端
    client = TapdataClient("http://10.49.4.153:3030")
    
    # 登录
    try:
        token = client.login("admin@admin.com", "abcd.1234")
        print("✓ 登录成功", token)
    except TapdataError as e:
        print(f"✗ 登录失败: {e.message}")
        return
    
    # 查询连接
    connections = client.connections.list(name="doubled")
    print(f"\n总共 {len(connections)} 个连接:")
    for conn in connections:
        print(f"  - {conn}")
    
    # 查询任务
    tasks = client.tasks.list(name="kpy")
    print(f"\n总共 {len(tasks)} 个任务:")
    for task in tasks:
        print(f"  -{task}")


def example_connection_management():
    """连接管理示例"""
    client = TapdataClient("http://10.49.4.153:3030")
    client.login("admin@admin.com", "abcd.1234")
    
    # 查询源连接
    print("\n=== 源连接 ===")
    source_connections = client.connections.list_source()
    for conn in source_connections:
        print(f"  {conn}")
    
    # 查询 MySQL 连接
    print("\n=== MySQL 连接 ===")
    mysql_connections = client.connections.list_mysql()
    for conn in mysql_connections:
        print(f"  {conn.name}: {conn.status}")
    
    # 查询有效连接
    print("\n=== 有效连接 ===")
    valid_connections = client.connections.list_valid()
    print(f"  共 {len(valid_connections)} 个有效连接")
    
    # 使用枚举过滤
    print("\n=== MySQL 源连接 ===")
    connections = client.connections.list(
        connection_type=ConnectionType.SOURCE,
        database_type=DatabaseType.MYSQL,
        status=Status.VALID
    )
    for conn in connections:
        print(f"  {conn.name}")


def example_task_management():
    """任务管理示例"""
    client = TapdataClient("http://10.49.4.153:3030")
    client.login("admin@admin.com", "abcd.1234")
    
    # 查询运行中的任务
    print("\n=== 运行中的任务 ===")
    running_tasks = client.tasks.list_running()
    
    if running_tasks:
        task = running_tasks[0]
        print(f"找到任务: {task.name} ({task.id})")
        
        # 停止任务
        print("\n停止任务...")
        try:
            result = client.tasks.stop(task.id)
            print("✓ 任务已停止")
            sleep(5)
            result = client.tasks.start(task.id)
            print("✓ 任务已开启", result)
        except TapdataError as e:
            print(f"✗ 停止失败: {e.message}")
    else:
        print("没有运行中的任务")
    
    # 查询所有任务
    all_tasks = client.tasks.list()
    print(f"\n总共 {len(all_tasks)} 个任务:")
    
    # 按状态分组
    from collections import defaultdict
    tasks_by_status = defaultdict(list)
    for task in all_tasks:
        tasks_by_status[task.status].append(task)
    
    for status, tasks in tasks_by_status.items():
        print(f"  {status}: {len(tasks)} 个")


def example_task_logs():
    """任务日志查询示例"""
    import time
    
    client = TapdataClient("http://10.49.4.153:3030")
    client.login("admin@admin.com", "abcd.1234")
    
    # 获取第一个任务
    tasks = client.tasks.list()
    if not tasks:
        print("没有可用的任务")
        return
    
    task = tasks[0]
    print(f"查询任务日志: {task.name}")
    
    if not task.task_record_id:
        print("任务没有 task_record_id")
        return
    
    # 查询最近一小时的日志
    end_time = int(time.time() * 1000)
    start_time = end_time - 50000  # 10秒前
    
    try:
        logs = client.tasks.get_logs(
            task_id=task.id,
            task_record_id=task.task_record_id,
            start=1769492201601,
            end=int(time.time()*1000),
            page=1,
            page_size=20
        )
        
        print(f"\n找到 {len(logs)} 条日志:")
        for log in logs:  # 只显示前5条
            print(f"  {log}")
    except TapdataError as e:
        print(f"✗ 查询日志失败: {e.message}")


def example_error_handling():
    """错误处理示例"""
    from tapdata_sdk import (
        TapdataAuthError,
        TapdataTimeoutError,
        TapdataConnectionError,
    )
    
    client = TapdataClient("http://10.49.4.153:3030")
    
    # 处理认证错误
    try:
        client.login("wrong@email.com", "wrong_password")
    except TapdataAuthError as e:
        print(f"认证失败: {e.message}")
        print(f"错误码: {e.code}")
    
    # 处理超时错误
    slow_client = TapdataClient(
        "http://10.49.4.153:3030",
        timeout=1  # 1秒超时
    )
    try:
        slow_client.get_timestamp()
    except TapdataTimeoutError as e:
        print(f"请求超时: {e.message}")
    
    # 处理连接错误
    bad_client = TapdataClient("http://invalid-host:3030")
    try:
        bad_client.get_timestamp()
    except TapdataConnectionError as e:
        print(f"连接错误: {e.message}")


def example_advanced_features():
    """高级功能示例"""
    # 使用已有的 token
    client = TapdataClient(
        base_url="http://10.49.4.153:3030",
        access_token="existing-token-here"
    )
    
    # 检查认证状态
    if client.is_authenticated():
        print("✓ 已认证")
    else:
        print("✗ 未认证，需要登录")
    
    # 自定义配置
    custom_client = TapdataClient(
        base_url="http://10.49.4.153:3030",
        timeout=60,  # 60秒超时
        verify_ssl=False  # 禁用 SSL 验证（仅用于开发）
    )
    
    # 使用完毕后登出
    client.logout()
    print("✓ 已登出")


if __name__ == "__main__":
    print("=" * 60)
    print("Tapdata SDK 示例")
    print("=" * 60)
    
    # 运行示例（取消注释想要运行的示例）
    #example_basic_usage()
    #example_connection_management()
    #example_task_management()
    example_task_logs()
    # example_error_handling()
    # example_advanced_features()
    
    print("\n请取消注释想要运行的示例函数")
