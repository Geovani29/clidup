"""
Base class for database handlers

Defines the interface that all database implementations must follow.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any


class DatabaseHandler(ABC):
    """Abstract base class for database backup and restore operations"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize database handler with configuration
        
        Args:
            config: Database configuration dictionary
        """
        self.config = config
        
    @abstractmethod
    def validate_tools(self) -> bool:
        """
        Validate that required database tools are installed
        
        Returns:
            True if all tools are available, raises exception otherwise
            
        Raises:
            RuntimeError: If required tools are not found
        """
        pass
    
    @abstractmethod
    def backup(self, database: str, output_file: Path) -> None:
        """
        Perform backup of specified database
        
        Args:
            database: Name of database to backup
            output_file: Path where backup file should be saved
            
        Raises:
            RuntimeError: If backup operation fails
        """
        pass
    
    @abstractmethod
    def restore(self, database: str, input_file: Path) -> None:
        """
        Restore database from backup file
        
        Args:
            database: Name of database to restore to
            input_file: Path to backup file
            
        Raises:
            RuntimeError: If restore operation fails
        """
        pass
