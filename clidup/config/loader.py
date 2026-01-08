"""
Configuration loader for clidup

Loads configuration from YAML file and environment variables.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class ConfigLoader:
    """Loads and validates configuration from YAML and environment variables"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration loader
        
        Args:
            config_path: Path to config.yaml file. If None, searches in current directory.
        """
        # Load environment variables from .env file
        load_dotenv()
        
        # Determine config file path
        if config_path is None:
            config_path = self._find_config_file()
        
        self.config_path = Path(config_path)
        self.config = self._load_yaml()
        
    def _find_config_file(self) -> Path:
        """Find config.yaml in current directory or parent directories"""
        current = Path.cwd()
        
        # Search current directory and up to 3 parent directories
        for _ in range(4):
            config_file = current / "config.yaml"
            if config_file.exists():
                return config_file
            current = current.parent
        
        raise FileNotFoundError(
            "config.yaml not found. Please create a config.yaml file in your project directory."
        )
    
    def _load_yaml(self) -> Dict[str, Any]:
        """Load and parse YAML configuration file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                return config if config else {}
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing config.yaml: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
    
    def get_postgres_config(self) -> Dict[str, Any]:
        """
        Get PostgreSQL configuration with password from environment
        
        Returns:
            Dictionary with host, port, username, database, and password
        """
        postgres = self.config.get('postgres', {})
        
        # Get password from environment variable
        password = os.getenv('POSTGRES_PASSWORD')
        if not password:
            raise ValueError(
                "POSTGRES_PASSWORD environment variable not set. "
                "Please set it in your .env file or environment."
            )
        
        return {
            'host': postgres.get('host', 'localhost'),
            'port': postgres.get('port', 5432),
            'username': postgres.get('username', 'postgres'),
            'database': postgres.get('database', 'postgres'),
            'password': password
        }
    
    def get_backup_directory(self) -> Path:
        """
        Get backup directory path and create if it doesn't exist
        
        Returns:
            Path object for backup directory
        """
        backup_config = self.config.get('backup', {})
        directory = backup_config.get('directory', './backups')
        
        backup_path = Path(directory)
        backup_path.mkdir(parents=True, exist_ok=True)
        
        return backup_path.resolve()
