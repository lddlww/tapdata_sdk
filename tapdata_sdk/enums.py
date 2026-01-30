"""Enumeration type definitions"""
from enum import Enum


class ConnectionType(str, Enum):
    """Connection type"""
    SOURCE = "source"
    TARGET = "target"
    
    def __str__(self):
        return self.value


class DatabaseType(str, Enum):
    """Database type"""
    MYSQL = "Mysql"
    CLICKHOUSE = "Clickhouse"
    MONGODB = "MongoDB"
    POSTGRESQL = "PostgreSQL"
    ORACLE = "Oracle"
    SQLSERVER = "SQLServer"
    
    def __str__(self):
        return self.value


class Status(str, Enum):
    """Status"""
    EDIT = "edit"
    WAIT_START = "wait_start"
    WAIT_RUN = "wait_run"
    RUNNING = "running"
    COMPLETE = "complete"
    STOPPING = "stopping"
    STOP = "stop"
    ERROR = "error"
    RENEWING = "renewing"
    RENEW_FAILED = "renew_failed"
    DELETING = "deleting"
    DELETE_FAILED = "delete_failed"
    DELETED = "deleted"
    VALID = "ready"
    INVALID = "invalid"
    TESTING = "testing"
    
    def __str__(self):
        return self.value


class LogLevel(str, Enum):
    """Log level"""
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    DEBUG = "DEBUG"
    
    def __str__(self):
        return self.value
