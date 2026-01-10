"""
MySQL database handler

Implements backup and restore operations using mysqldump and mysql.
"""

import subprocess
import shutil
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from datetime import datetime
import os

from .base import DatabaseHandler


logger = logging.getLogger("clidup")


class MySQLHandler(DatabaseHandler):
    """MySQL backup and restore implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MySQL handler
        
        Args:
            config: MySQL configuration with host, port, username, password, database
        """
        super().__init__(config)
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 3306)
        self.username = config.get('username', 'root')
        self.password = config.get('password', '')
        self.default_database = config.get('database', '')
        
    def validate_tools(self) -> bool:
        """
        Validate that mysqldump and mysql are installed and accessible
        
        Returns:
            True if tools are available
            
        Raises:
            RuntimeError: If required tools are not found
        """
        # Check for mysqldump
        if not shutil.which('mysqldump'):
            raise RuntimeError(
                "mysqldump not found. Please install MySQL client tools.\n"
                "Download from: https://dev.mysql.com/downloads/"
            )
        
        # Check for mysql
        if not shutil.which('mysql'):
            raise RuntimeError(
                "mysql client not found. Please install MySQL client tools.\n"
                "Download from: https://dev.mysql.com/downloads/"
            )
        
        logger.debug("MySQL tools validated successfully")
        
        # Test connection to MySQL
        self.validate_connection()
        
        return True
    
    def validate_connection(self) -> bool:
        """
        Validate connection to MySQL server
        
        Returns:
            True if connection successful
            
        Raises:
            RuntimeError: If connection fails
        """
        logger.debug(f"Testing connection to MySQL at {self.host}:{self.port}")
        
        cmd = [
            'mysql',
            '-h', self.host,
            '-P', str(self.port),
            '-u', self.username,
            '-e', 'SELECT 1;'
        ]
        
        try:
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=10,
                check=True
            )
            logger.debug("MySQL connection test successful")
            return True
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Connection timeout to MySQL at {self.host}:{self.port}. "
                f"Check that the server is running and network is accessible."
            )
        except subprocess.CalledProcessError as e:
            error_detail = e.stderr.strip() if e.stderr else str(e)
            raise RuntimeError(
                f"Cannot connect to MySQL at {self.host}:{self.port}. "
                f"Check credentials and server status. Error: {error_detail}"
            )

    def get_default_backup_name(self, database: str) -> str:
        """
        Get default backup filename for MySQL
        
        Args:
            database: Name of database
            
        Returns:
            Filename string
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return f"mysql_{database}_full_{timestamp}.sql"
    
    def _get_env(self) -> Dict[str, str]:
        """
        Get environment variables for MySQL commands
        
        Returns:
            Dictionary with MYSQL_PWD set
        """
        env = os.environ.copy()
        if self.password:
            env['MYSQL_PWD'] = self.password
        return env
    
    def _database_exists(self, database: str) -> bool:
        """
        Check if a database exists in MySQL
        
        Args:
            database: Name of database to check
            
        Returns:
            True if database exists, False otherwise
        """
        cmd = [
            'mysql',
            '-h', self.host,
            '-P', str(self.port),
            '-u', self.username,
            '-e', f"SHOW DATABASES LIKE '{database}';"
        ]
        
        try:
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=10,
                check=True
            )
            
            # If output contains header and row, database exists
            # Example output:
            # Database (test_db)
            # test_db
            return len(result.stdout.strip().split('\n')) > 1
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            logger.warning(f"Could not check if database '{database}' exists")
            return True  # Assume it exists to avoid blocking restore
    
    def backup(self, database: str, output_file: Path) -> None:
        """
        Perform MySQL backup using mysqldump
        
        Args:
            database: Name of database to backup
            output_file: Path where backup file should be saved
            
        Raises:
            RuntimeError: If backup operation fails
        """
        logger.info(f"Starting MySQL backup of database '{database}'")
        
        # Build mysqldump command
        cmd = [
            'mysqldump',
            '-h', self.host,
            '-P', str(self.port),
            '-u', self.username,
            '--result-file', str(output_file),
            database
        ]
        
        try:
            # Run mysqldump
            # Note: mysqldump usually doesn't output much to stdout/stderr unless there's an error
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=3600,
                check=True
            )
            
            logger.debug(f"mysqldump completed successfully")
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Backup failed: {e.stderr if e.stderr else str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during backup: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
    
    def restore(self, database: str, input_file: Path) -> None:
        """
        Restore MySQL database using mysql client
        
        Args:
            database: Name of database to restore to
            input_file: Path to backup file
            
        Raises:
            RuntimeError: If restore operation fails
        """
        logger.info(f"Starting MySQL restore to database '{database}'")
        
        # Build mysql command - read from file using shell redirection logic is tricky with subprocess
        # Standard way: mysql -u user -p dbname < file.sql
        # With subprocess, we can open the file and pass it to stdin
        
        cmd = [
            'mysql',
            '-h', self.host,
            '-P', str(self.port),
            '-u', self.username,
            database
        ]
        
        try:
            with open(input_file, 'r') as f:
                result = subprocess.run(
                    cmd,
                    env=self._get_env(),
                    stdin=f,
                    capture_output=True,
                    text=True,
                    timeout=3600,
                    check=True
                )
            
            logger.debug(f"mysql restore completed successfully")
            
        except FileNotFoundError:
             raise RuntimeError(f"Backup file not found: {input_file}")
        except subprocess.CalledProcessError as e:
            error_msg = f"Restore failed: {e.stderr if e.stderr else str(e)}"
            # Check for "Unknown database" error
            if "Unknown database" in error_msg:
                 error_msg += f"\nHint: Create the database first with: CREATE DATABASE {database};"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during restore: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
