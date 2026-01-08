"""
PostgreSQL database handler

Implements backup and restore operations using pg_dump and psql.
"""

import subprocess
import shutil
from pathlib import Path
from typing import Dict, Any
import logging

from .base import DatabaseHandler


logger = logging.getLogger("clidup")


class PostgresHandler(DatabaseHandler):
    """PostgreSQL backup and restore implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL handler
        
        Args:
            config: PostgreSQL configuration with host, port, username, password, database
        """
        super().__init__(config)
        self.host = config['host']
        self.port = config['port']
        self.username = config['username']
        self.password = config['password']
        self.default_database = config['database']
        
    def validate_tools(self) -> bool:
        """
        Validate that pg_dump and psql are installed and accessible
        
        Returns:
            True if tools are available
            
        Raises:
            RuntimeError: If required tools are not found
        """
        # Check for pg_dump
        if not shutil.which('pg_dump'):
            raise RuntimeError(
                "pg_dump not found. Please install PostgreSQL client tools.\n"
                "Download from: https://www.postgresql.org/download/"
            )
        
        # Check for psql
        if not shutil.which('psql'):
            raise RuntimeError(
                "psql not found. Please install PostgreSQL client tools.\n"
                "Download from: https://www.postgresql.org/download/"
            )
        
        logger.debug("PostgreSQL tools validated successfully")
        
        # Test connection to PostgreSQL
        self.test_connection()
        
        return True
    
    def test_connection(self) -> bool:
        """
        Test connection to PostgreSQL server
        
        Returns:
            True if connection successful
            
        Raises:
            RuntimeError: If connection fails
        """
        logger.debug(f"Testing connection to PostgreSQL at {self.host}:{self.port}")
        
        cmd = [
            'psql',
            '-h', self.host,
            '-p', str(self.port),
            '-U', self.username,
            '-d', self.default_database,
            '-c', 'SELECT 1;'
        ]
        
        try:
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=10,  # 10 second timeout
                check=True
            )
            logger.debug("PostgreSQL connection test successful")
            return True
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Connection timeout to PostgreSQL at {self.host}:{self.port}. "
                f"Check that the server is running and network is accessible."
            )
        except subprocess.CalledProcessError as e:
            error_detail = e.stderr.strip() if e.stderr else str(e)
            raise RuntimeError(
                f"Cannot connect to PostgreSQL at {self.host}:{self.port}. "
                f"Check credentials and server status. Error: {error_detail}"
            )
    
    def _get_env(self) -> Dict[str, str]:
        """
        Get environment variables for PostgreSQL commands
        
        Returns:
            Dictionary with PGPASSWORD set
        """
        import os
        env = os.environ.copy()
        env['PGPASSWORD'] = self.password
        return env
    
    def _database_exists(self, database: str) -> bool:
        """
        Check if a database exists in PostgreSQL
        
        Args:
            database: Name of database to check
            
        Returns:
            True if database exists, False otherwise
        """
        cmd = [
            'psql',
            '-h', self.host,
            '-p', str(self.port),
            '-U', self.username,
            '-d', self.default_database,
            '-lqt'  # List databases in quiet mode
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
            
            # Check if database name appears in output
            databases = [line.split('|')[0].strip() for line in result.stdout.split('\n')]
            return database in databases
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            logger.warning(f"Could not check if database '{database}' exists")
            return True  # Assume it exists to avoid blocking restore
    
    def backup(self, database: str, output_file: Path) -> None:
        """
        Perform PostgreSQL backup using pg_dump
        
        Args:
            database: Name of database to backup
            output_file: Path where backup SQL file should be saved
            
        Raises:
            RuntimeError: If backup fails
        """
        logger.info(f"Starting PostgreSQL backup of database '{database}'")
        
        # Build pg_dump command
        cmd = [
            'pg_dump',
            '-h', self.host,
            '-p', str(self.port),
            '-U', self.username,
            '-F', 'p',  # Plain text format
            '-f', str(output_file),
            database
        ]
        
        try:
            # Run pg_dump
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout for large backups
                check=True
            )
            
            logger.debug(f"pg_dump completed successfully")
            
            if result.stderr:
                # pg_dump may output warnings to stderr even on success
                logger.debug(f"pg_dump stderr: {result.stderr}")
                
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
        Restore PostgreSQL database using psql
        
        Args:
            database: Name of database to restore to
            input_file: Path to backup SQL file
            
        Raises:
            RuntimeError: If restore fails
        """
        logger.info(f"Starting PostgreSQL restore to database '{database}'")
        
        # Validate database exists
        if not self._database_exists(database):
            raise RuntimeError(
                f"Database '{database}' does not exist. "
                f"Create it first with: CREATE DATABASE {database};"
            )
        
        # Build psql command
        cmd = [
            'psql',
            '-h', self.host,
            '-p', str(self.port),
            '-U', self.username,
            '-d', database,
            '-f', str(input_file)
        ]
        
        try:
            # Run psql
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout for large restores
                check=True
            )
            
            logger.debug(f"psql completed successfully")
            
            if result.stderr:
                # psql may output notices to stderr even on success
                logger.debug(f"psql stderr: {result.stderr}")
                
        except subprocess.CalledProcessError as e:
            error_msg = f"Restore failed: {e.stderr if e.stderr else str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during restore: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
