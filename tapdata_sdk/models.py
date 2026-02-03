"""Data model definitions"""
from dataclasses import dataclass
from typing import Optional, List, Dict


@dataclass
class Connection:
    """Connection model"""
    id: str
    name: str
    connection_type: str
    database_type: Optional[str]
    status: str
    endpoint: Optional[str]
    port: Optional[str]
    database: Optional[str]
    user: Optional[str]

    @classmethod
    def from_dict(cls, data: dict) -> "Connection":
        """Create connection object from API response"""
        config = data.get("config", {})
        uri = config.get("uri")
        host = config.get("host")
        
        return cls(
            id=data["id"],
            name=data["name"],
            connection_type=data["connection_type"],
            database_type=data.get("database_type"),
            status=data["status"],
            endpoint=uri or host,
            user=config.get("user",""),
            database=config.get("database",""),
            port=config.get("port","")
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "connection_type": self.connection_type,
            "database_type": self.database_type,
            "status": self.status,
            "endpoint": self.endpoint,
            "port": self.port,
            "database": self.database,
            "user": self.user
        }


@dataclass
class Task:
    """Task model"""
    id: str
    name: str
    type: str
    status: str
    task_record_id: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        """Create task object from API response"""
        return cls(
            id=data["id"],
            name=data["name"],
            type=data["type"],
            status=data["status"],
            task_record_id=data.get("taskRecordId"),
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "status": self.status,
            "taskRecordId": self.task_record_id,
        }

@dataclass
class TaskDetail:
    """Task detail model"""
    id: str
    name: str
    type: str
    status: str
    task_record_id: str
    nodes: List[dict]

    @classmethod
    def from_dict(cls, data: dict) -> "TaskDetail":
        """Create task object from API response"""
        nodes = []
        for node in data.get("dag",{}).get("nodes",[]):
            attrs = node.get("attrs",{})
            nodes.append({
                "id": node.get("id"),
                "name": node.get("name"),
                "connectionId": node.get("connectionId"),
                "connectionName": attrs.get("connectionName"),
                "connectionType": attrs.get("__connectionType"),
                "syncObjects": node.get("syncObjects",[])
            })
        return cls(
            id=data["id"],
            name=data["name"],
            type=data["type"],
            status=data["status"],
            task_record_id=data.get("taskRecordId"),
            nodes=nodes
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "status": self.status,
            "taskRecordId": self.task_record_id,
            "nodes": self.nodes
        }

@dataclass
class TaskRelation:
    """
    Task connection relationship mapping model
    """
    source_connection_id: Optional[str] = None
    target_connection_id: Optional[str] = None
    table_name_relation: Optional[Dict[str, str]] = None
    source_conn: Optional[Connection] = None
    target_conn: Optional[Connection] = None

    @classmethod
    def from_dict(cls, data: dict) -> "TaskRelation":
        """
        Create TaskRelation from task detail dictionary

        Args:
            data: Raw task data from API (containing 'dag' and 'nodes')
        """
        nodes = data.get("nodes", [])

        if len(nodes) < 2:
            return cls()

        source_node = nodes[0]
        target_node = nodes[-1]

        relations = {}
        for obj in target_node.get("syncObjects", []):
            if "tableNameRelation" in obj:
                relations.update(obj["tableNameRelation"])

        return cls(
            source_connection_id=source_node.get("connectionId"),
            target_connection_id=target_node.get("connectionId"),
            table_name_relation=relations
        )

    def to_dict(self) -> dict:
        """
        Convert to dictionary including nested connection details
        """
        return {
            "source_connection_id": self.source_connection_id,
            "target_connection_id": self.target_connection_id,
            "table_name_relation": self.table_name_relation,
            "source": self.source_conn.to_dict() if self.source_conn else None,
            "target": self.target_conn.to_dict() if self.target_conn else None
        }

@dataclass
class TaskLog:
    """Task log"""
    task_id: str
    task_record_id: str
    task_name: str
    node_id: str
    node_name: str
    level: str
    message: str
    timestamp: int
    date: str

    @classmethod
    def from_dict(cls, data: dict) -> "TaskLog":
        """Create log object from API response"""
        return cls(
            task_id=data["taskId"],
            task_record_id=data["taskRecordId"],
            task_name=data["taskName"],
            node_name=data.get("nodeName",""),
            node_id=data.get("nodeId",""),
            level=data["level"],
            message=data["message"],
            timestamp=data["timestamp"],
            date=data["date"],
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "task_id": self.task_id,
            "task_record_id": self.task_record_id,
            "task_name": self.task_name,
            "node_id": self.node_id,
            "node_name": self.node_name,
            "level": self.level,
            "message": self.message,
            "timestamp": self.timestap,
            "date": self.date
        }
