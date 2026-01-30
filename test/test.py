"""
单元测试示例
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from tapdata_sdk import (
    TapdataClient,
    ConnectionType,
    DatabaseType,
    Status,
    TapdataError,
    TapdataAuthError,
)
from tapdata_sdk.models import Connection, Task


class TestTapdataClient:
    """测试 TapdataClient"""
    
    def test_init(self):
        """测试初始化"""
        client = TapdataClient("http://localhost:3030")
        assert client.base_url == "http://localhost:3030"
        assert client.access_token is None
        assert client.timeout == 30
    
    def test_init_with_token(self):
        """测试使用 token 初始化"""
        client = TapdataClient(
            "http://localhost:3030",
            access_token="test-token"
        )
        assert client.access_token == "test-token"
    
    @patch('requests.Session.request')
    def test_login_success(self, mock_request):
        """测试登录成功"""
        # Mock 时间戳请求
        mock_timestamp_response = Mock()
        mock_timestamp_response.json.return_value = {
            "code": "ok",
            "data": 1234567890
        }
        
        # Mock 登录请求
        mock_login_response = Mock()
        mock_login_response.json.return_value = {
            "code": "ok",
            "data": {"id": "test-token"}
        }
        
        mock_request.side_effect = [
            mock_timestamp_response,
            mock_login_response
        ]
        
        client = TapdataClient("http://localhost:3030")
        token = client.login("test@example.com", "password")
        
        assert token == "test-token"
        assert client.access_token == "test-token"
    
    @patch('requests.Session.request')
    def test_login_failure(self, mock_request):
        """测试登录失败"""
        mock_timestamp_response = Mock()
        mock_timestamp_response.json.return_value = {
            "code": "ok",
            "data": 1234567890
        }
        
        mock_login_response = Mock()
        mock_login_response.json.return_value = {
            "code": "UNAUTHORIZED",
            "message": "Invalid credentials"
        }
        
        mock_request.side_effect = [
            mock_timestamp_response,
            mock_login_response
        ]
        
        client = TapdataClient("http://localhost:3030")
        
        with pytest.raises(TapdataAuthError) as exc_info:
            client.login("test@example.com", "wrong-password")
        
        assert exc_info.value.code == "UNAUTHORIZED"
    
    def test_logout(self):
        """测试登出"""
        client = TapdataClient(
            "http://localhost:3030",
            access_token="test-token"
        )
        
        client.logout()
        assert client.access_token is None
    
    def test_is_authenticated(self):
        """测试认证状态检查"""
        client = TapdataClient("http://localhost:3030")
        assert client.is_authenticated() is False
        
        client.access_token = "test-token"
        assert client.is_authenticated() is True


class TestConnectionClient:
    """测试 ConnectionClient"""
    
    @patch('requests.Session.request')
    def test_list_connections(self, mock_request):
        """测试查询连接列表"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "code": "ok",
            "data": {
                "items": [
                    {
                        "id": "conn1",
                        "name": "MySQL Source",
                        "connection_type": "source",
                        "database_type": "MySQL",
                        "status": "complete",
                        "config": {"host": "localhost:3306"}
                    }
                ]
            }
        }
        mock_request.return_value = mock_response
        
        client = TapdataClient(
            "http://localhost:3030",
            access_token="test-token"
        )
        
        connections = client.connections.list()
        
        assert len(connections) == 1
        assert isinstance(connections[0], Connection)
        assert connections[0].name == "MySQL Source"
        assert connections[0].database_type == "MySQL"
    
    @patch('requests.Session.request')
    def test_list_source_connections(self, mock_request):
        """测试查询源连接"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "code": "ok",
            "data": {"items": []}
        }
        mock_request.return_value = mock_response
        
        client = TapdataClient(
            "http://localhost:3030",
            access_token="test-token"
        )
        
        connections = client.connections.list_source()
        
        # 验证调用参数包含 connection_type 过滤
        call_args = mock_request.call_args
        params = call_args.kwargs.get('params', {})
        filter_dict = params.get('filter', {})
        where = filter_dict.get('where', {})
        
        assert where.get('connection_type') == ConnectionType.SOURCE


class TestTaskClient:
    """测试 TaskClient"""
    
    @patch('requests.Session.request')
    def test_list_tasks(self, mock_request):
        """测试查询任务列表"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "code": "ok",
            "data": {
                "items": [
                    {
                        "id": "task1",
                        "name": "Sync Task",
                        "type": "sync",
                        "status": "running",
                        "taskRecordId": "record1"
                    }
                ]
            }
        }
        mock_request.return_value = mock_response
        
        client = TapdataClient(
            "http://localhost:3030",
            access_token="test-token"
        )
        
        tasks = client.tasks.list()
        
        assert len(tasks) == 1
        assert isinstance(tasks[0], Task)
        assert tasks[0].name == "Sync Task"
        assert tasks[0].status == "running"
    
    @patch('requests.Session.request')
    def test_start_task(self, mock_request):
        """测试启动任务"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "code": "ok",
            "data": {"success": True}
        }
        mock_request.return_value = mock_response
        
        client = TapdataClient(
            "http://localhost:3030",
            access_token="test-token"
        )
        
        result = client.tasks.start("task1")
        
        assert result["code"] == "ok"
        
        # 验证调用了正确的 API
        call_args = mock_request.call_args
        assert "/api/Task/batchStart" in call_args.args[1]
    
    @patch('requests.Session.request')
    def test_stop_task(self, mock_request):
        """测试停止任务"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "code": "ok",
            "data": {"success": True}
        }
        mock_request.return_value = mock_response
        
        client = TapdataClient(
            "http://localhost:3030",
            access_token="test-token"
        )
        
        result = client.tasks.stop("task1")
        
        assert result["code"] == "ok"


class TestModels:
    """测试数据模型"""
    
    def test_connection_from_dict(self):
        """测试从字典创建 Connection"""
        data = {
            "id": "conn1",
            "name": "Test Connection",
            "connection_type": "source",
            "database_type": "MySQL",
            "status": "complete",
            "config": {"host": "localhost"}
        }
        
        conn = Connection.from_dict(data)
        
        assert conn.id == "conn1"
        assert conn.name == "Test Connection"
        assert conn.endpoint == "localhost"
    
    def test_task_from_dict(self):
        """测试从字典创建 Task"""
        data = {
            "id": "task1",
            "name": "Test Task",
            "type": "sync",
            "status": "running",
            "taskRecordId": "record1"
        }
        
        task = Task.from_dict(data)
        
        assert task.id == "task1"
        assert task.name == "Test Task"
        assert task.task_record_id == "record1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
