"""Tapdata API Client"""
import logging
from typing import Dict, List, Optional, Union
from urllib.parse import urljoin
import urllib.parse
import json as jsonx
import time

import requests

from .exceptions import (
    TapdataAuthError,
    TapdataConnectionError,
    TapdataError,
    TapdataTimeoutError,
)
from .models import Connection, Task, TaskDetail, TaskLog
from .utils import rc4_encrypt, gen_sign, build_filter
from .enums import ConnectionType, DatabaseType, Status, LogLevel


logger = logging.getLogger(__name__)


class TapdataClient:
    """
    Tapdata API Client
    
    Examples:
        >>> client = TapdataClient("http://localhost:3030")
        >>> client.login("admin@test.com", "password")
        >>> connections = client.connections.list()
        >>> tasks = client.tasks.list()
    """
    
    DEFAULT_TIMEOUT = 30
    DEFAULT_SECRET = "Gotapd8"
    
    def __init__(
        self,
        base_url: str,
        access_token: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        verify_ssl: bool = True,
    ):
        """
        Initialize client
        
        Args:
            base_url: API base URL
            access_token: Access token (optional)
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificate
        """
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        
        # Initialize sub-clients
        self.connections = ConnectionClient(self)
        self.tasks = TaskClient(self)
    
    def _build_url(self, path: str) -> str:
        """Build complete URL"""
        return urljoin(self.base_url, path)
    
    def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        json: Optional[dict] = None,
        **kwargs,
    ) -> dict:
        """
        Send HTTP request
        
        Args:
            method: HTTP method
            path: API path
            params: URL parameters
            json: JSON request body
            **kwargs: Other request parameters
            
        Returns:
            Response data
            
        Raises:
            TapdataError: API error
            TapdataTimeoutError: Request timeout
            TapdataConnectionError: Connection error
        """
        params = params or {}
        
        # Add access_token
        url = self._build_url(path)
        if self.access_token:
            params["access_token"] = self.access_token
        
        if params.get('filter'):
           filter_str = jsonx.dumps(params.get('filter'), separators=(',', ':'))
           params['filter'] = filter_str
        
        try:
            logger.debug(f"Request: {method} {url}")
            
            resp = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=kwargs.get("timeout", self.timeout),
                verify=self.verify_ssl,
                **{k: v for k, v in kwargs.items() if k != "timeout"},
            )
            resp.raise_for_status()
            
            data = resp.json()
            
            # Check business status code
            if data.get("code") != "ok":
                error_code = data.get("code")
                if error_code in ["UNAUTHORIZED", "FORBIDDEN"]:
                    raise TapdataAuthError(data)
                raise TapdataError(data)
            
            logger.debug(f"Response: {data.get('code')}")
            return data
            
        except requests.exceptions.Timeout as e:
            raise TapdataTimeoutError({"message": f"Request timeout: {e}"})
        except requests.exceptions.ConnectionError as e:
            raise TapdataConnectionError({"message": f"Connection error: {e}"})
        except requests.exceptions.RequestException as e:
            raise TapdataError({"message": f"Request failed: {e}"})
    
    def get_timestamp(self) -> int:
        """
        Get server timestamp
        
        Returns:
            Server timestamp
        """
        resp = self._request("GET", "/api/timeStamp")
        return resp["data"]
    
    def login(
        self,
        email: str,
        password: str,
        secret: str = DEFAULT_SECRET,
    ) -> str:
        """
        User login
        
        Args:
            email: Email
            password: Password (plain text)
            secret: Encryption secret key
            
        Returns:
            Access token
            
        Examples:
            >>> client = TapdataClient("http://localhost:3030")
            >>> token = client.login("admin@test.com", "password")
        """
        stime = self.get_timestamp()
        enc_pwd = rc4_encrypt(password, secret)
        sign = gen_sign(email, enc_pwd, stime, secret)
        
        resp = self._request(
            "POST",
            "/api/users/login",
            json={
                "email": email,
                "password": enc_pwd,
                "sign": sign,
            },
        )
        
        self.access_token = resp["data"]["id"]
        logger.info(f"Login successful: {email}")
        
        return self.access_token
    
    def logout(self) -> None:
        """Logout"""
        self.access_token = None
        self.session.close()
        self.session = requests.Session()
        logger.info("Logged out")
    
    def is_authenticated(self) -> bool:
        """Check if authenticated"""
        return self.access_token is not None


class ConnectionClient:
    """Connection management client"""
    
    def __init__(self, client: TapdataClient):
        self.client = client
    
    def list(
        self,
        connection_type: Optional[Union[str, ConnectionType]] = None,
        database_type: Optional[Union[str, DatabaseType]] = None,
        status: Optional[Union[str, Status]] = None,
        name: Optional[str] = None,
        skip: int = 0,
        limit: int = 20,
    ) -> List[Connection]:
        """
        Query connection list
        
        Args:
            connection_type: Connection type
            database_type: Database type
            status: Status
            skip: Number of records to skip
            limit: Limit on number of results
            
        Returns:
            Connection list
            
        Examples:
            >>> connections = client.connections.list(
            ...     connection_type=ConnectionType.SOURCE,
            ...     database_type=DatabaseType.MYSQL
            ... )
        """
        where = {"createType": {"$ne": "System"}}
        order = "last_updated DESC"
        
        if connection_type:
            where["connection_type"] = str(connection_type)
        if database_type:
            where["database_type"] = str(database_type)
        if status:
            where["status"] = str(status)
        if name:
            where["name"] = {"like":str(name),"options":"i"}
        
        resp = self.client._request(
            "GET",
            "/api/Connections",
            params={"filter": build_filter(order=order, skip=skip, limit=limit, where=where)},
        )
        
        return [Connection.from_dict(item) for item in resp["data"]["items"]]
    
    def get(self, connection_id: str) -> Connection:
        """
        Get single connection details
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Connection object
        """
        resp = self.client._request("GET", f"/api/Connections/{connection_id}")
        return Connection.from_dict(resp["data"])
    
    def list_source(self) -> List[Connection]:
        """Get all source connections"""
        return self.list(connection_type=ConnectionType.SOURCE)
    
    def list_target(self) -> List[Connection]:
        """Get all target connections"""
        return self.list(connection_type=ConnectionType.TARGET)
    
    def list_mysql(self) -> List[Connection]:
        """Get all MySQL connections"""
        return self.list(database_type=DatabaseType.MYSQL)
    
    def list_clickhouse(self) -> List[Connection]:
        """Get all ClickHouse connections"""
        return self.list(database_type=DatabaseType.CLICKHOUSE)
    
    def list_mongodb(self) -> List[Connection]:
        """Get all MongoDB connections"""
        return self.list(database_type=DatabaseType.MONGODB)
    
    def list_valid(self) -> List[Connection]:
        """Get all valid connections"""
        return self.list(status=Status.VALID)
    
    def list_invalid(self) -> List[Connection]:
        """Get all invalid connections"""
        return self.list(status=Status.INVALID)

    def list_testing(self) -> List[Connection]:
        """Get all testing connections"""
        return self.list(status=Status.TESTING)

class TaskClient:
    """Task management client"""
    
    def __init__(self, client: TapdataClient):
        self.client = client
    
    def list(
        self,
        status: Optional[Union[str, Status]] = None,
        name: Optional[str] = None,
        skip: int = 0,
        limit: int = 20,
    ) -> List[Task]:
        """
        Query task list
        
        Args:
            status: Status filter
            skip: Number of records to skip
            limit: Limit on number of results
            
        Returns:
            Task list
        """
        where = {}
        if status:
            where["status"] = str(status)

        if name:
            where["name"] = {"like": str(name),"options":"i"}
        
        fields = {
            "id": True,
            "name": True,
            "type": True,
            "status": True,
            "taskRecordId": True,
        }
        
        resp = self.client._request(
            "GET",
            "/api/Task",
            params={
                "filter": build_filter(
                    skip=skip,
                    limit=limit,
                    where=where,
                    fields=fields,
                )
            },
        )
        
        return [Task.from_dict(item) for item in resp["data"]["items"]]
    
    def get(self, task_id: str) -> TaskDetail:
        """
        Get single task details
        
        Args:
            task_id: Task ID
            
        Returns:
            Task object
        """
        resp = self.client._request("GET", f"/api/Task/{task_id}")
        return TaskDetail.from_dict(resp["data"])
    
    def list_running(self) -> List[Task]:
        """Get all running tasks"""
        return self.list(status=Status.RUNNING)

    def list_error(self) -> List[Task]:
        """Get all error tasks"""
        return self.list(status=Status.ERROR)
    
    def start(self, task_id: str) -> dict:
        """
        Start task
        
        Args:
            task_id: Task ID
            
        Returns:
            Operation result
        """
        logger.info(f"Starting task: {task_id}")
        return self.client._request(
            "PUT",
            "/api/Task/batchStart",
            params={"taskIds": task_id},
        )
    
    def stop(self, task_id: str) -> dict:
        """
        Stop task
        
        Args:
            task_id: Task ID
            
        Returns:
            Operation result
        """
        logger.info(f"Stopping task: {task_id}")
        return self.client._request(
            "PUT",
            "/api/Task/batchStop",
            params={"taskIds": task_id},
        )
    
    def reset(self, task_id: str) -> dict:
        """
        Reset task
        
        Args:
            task_id: Task ID
            
        Returns:
            Operation result
        """
        logger.info(f"Resetting task: {task_id}")
        return self.client._request(
            "PATCH",
            "/api/Task/batchRenew",
            params={"taskIds": task_id},
        )
    
    def delete(self, task_id: str) -> dict:
        """
        Delete task
        
        Args:
            task_id: Task ID
            
        Returns:
            Operation result
        """
        logger.warning(f"Deleting task: {task_id}")
        return self.client._request(
            "DELETE",
            "/api/Task/batchDelete",
            params={"taskIds": task_id},
        )
    
    def get_logs(
        self,
        task_id: str,
        task_record_id: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
        levels: Optional[List[Union[str, LogLevel]]] = None,
    ) -> List[TaskLog]:
        """
        Get task logs
        
        Args:
            task_id: Task ID
            task_record_id: Task record ID
            start: Start timestamp
            end: End timestamp
            page: Page number
            page_size: Items per page
            levels: Log level filter
            
        Returns:
            Log data
        """
        if start is None:
            start = int(time.time()*1000)-1000

        if end is None:
            end = start + 2000

        if levels is None:
            levels = [LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR]
        
        resp = self.client._request(
            "POST",
            "/api/MonitoringLogs/query",
            json={
                "taskId": task_id,
                "taskRecordId": task_record_id,
                "start": start,
                "end": end,
                "page": page,
                "pageSize": page_size,
                "order": "asc",
                "levels": [str(level) for level in levels],
            },
        )

        return [TaskLog.from_dict(item) for item in resp["data"]["items"]]
