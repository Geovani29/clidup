"""
SQLite database handler

Implements backup and restore operations using file copy.
"""

import shutil
from pathlib import Path
from typing import Dict, Any
import logging
from datetime import datetime
import os

from .base import DatabaseHandler


logger = logging.getLogger("clidup")


class SQLiteHandler(DatabaseHandler):
    """SQLite backup and restore implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLite handler
        
        Args:
            config: SQLite configuration with db_path
        """
        super().__init__(config)
        self.db_path = Path(config.get('db_path', 'data.db'))
        
    def validate_tools(self) -> bool:
        """
        Validate tools for SQLite (none required, just file access)
        
        Returns:
            True always
        """
        return True
    
    def validate_connection(self) -> bool:
        """
        Validate access to SQLite database file
        
        Returns:
            True if file exists and is accessible
            
        Raises:
            RuntimeError: If file is not accessible
        """
        # If file doesn't exist yet, that's okay for init but not for backup
        # However, for validate_connection, we usually check if we can interact with it
        # Here we just check if the path is valid/writable
        
        # Check if parent directory exists and is writable
        if not self.db_path.parent.exists():
            raise RuntimeError(f"Directory {self.db_path.parent} does not exist")
            
        if self.db_path.exists():
            if not os.access(self.db_path, os.R_OK):
                raise RuntimeError(f"Database file {self.db_path} is not readable")
        
        logger.debug(f"SQLite database path validated: {self.db_path}")
        return True

    def get_default_backup_name(self, database: str) -> str:
        """
        Get default backup filename for SQLite
        
        Args:
            database: Name of database (filename without extension, or ignored)
            
        Returns:
            Filename string
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        name = self.db_path.stem
        # If database arg is provided and different from stem, use it, otherwise use stem
        if database and database != "sqlite":
             name = database
             
        return f"sqlite_{name}_full_{timestamp}.db"
    
    def backup(self, database: str, output_file: Path) -> None:
        """
        Perform SQLite backup using file copy
        
        Args:
            database: Name of database (ignored for SQLite single file)
            output_file: Path where backup file should be saved
            
        Raises:
            RuntimeError: If backup operation fails
        """
        logger.info(f"Starting SQLite backup of '{self.db_path}'")
        
        if not self.db_path.exists():
             raise RuntimeError(f"Database file {self.db_path} does not exist")
             
        try:
            # Simple file copy
            shutil.copy2(self.db_path, output_file)
            logger.debug(f"File copied successfully to {output_file}")
            
        except Exception as e:
            error_msg = f"Unexpected error during backup: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
    
    def restore(self, database: str, input_file: Path) -> None:
        """
        Restore SQLite database by replacing the file
        
        Args:
            database: Name of database (ignored)
            input_file: Path to backup file
            
        Raises:
            RuntimeError: If restore operation fails
        """
        logger.info(f"Starting SQLite restore to '{self.db_path}'")
        
        try:
            # Backup current file just in case? Maybe too complex for now.
            # Just overwrite as requested.
            
            shutil.copy2(input_file, self.db_path)
            logger.debug(f"File restored successfully from {input_file}")
            
        except FileNotFoundError:
             raise RuntimeError(f"Backup file not found: {input_file}")
        except Exception as e:
            error_msg = f"Unexpected error during restore: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
