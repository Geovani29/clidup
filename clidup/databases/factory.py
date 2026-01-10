"""
Database handler factory

Creates and returns the appropriate database handler instance based on type.
"""

from typing import Dict, Any, Type
import logging

from .base import DatabaseHandler
from .postgres import PostgresHandler
from .mysql import MySQLHandler
from .sqlite import SQLiteHandler
from .mongodb import MongoDBHandler


logger = logging.getLogger("clidup")


class DatabaseFactory:
    """Factory for creating database handlers"""
    
    _handlers: Dict[str, Type[DatabaseHandler]] = {
        'postgres': PostgresHandler,
        'mysql': MySQLHandler,
        'sqlite': SQLiteHandler,
        'mongodb': MongoDBHandler
    }
    
    @classmethod
    def get_handler(cls, db_type: str, config: Dict[str, Any]) -> DatabaseHandler:
        """
        Get database handler instance
        
        Args:
            db_type: Type of database ('postgres', 'mysql', 'sqlite', 'mongodb')
            config: Database configuration dictionary
            
        Returns:
            Instance of DatabaseHandler
            
        Raises:
            ValueError: If database type is not supported
        """
        handler_cls = cls._handlers.get(db_type)
        
        if not handler_cls:
            raise ValueError(f"Unsupported database type: {db_type}. Supported types: {list(cls._handlers.keys())}")
            
        return handler_cls(config)
