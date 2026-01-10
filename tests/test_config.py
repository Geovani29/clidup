import pytest
from pathlib import Path
from unittest.mock import patch
from clidup.config.loader import ConfigLoader

def test_load_config(mock_config_file, mock_env_vars):
    """Test loading configuration from file and env vars"""
    loader = ConfigLoader(str(mock_config_file))
    
    # Check Postgres config
    pg_config = loader.get_postgres_config()
    assert pg_config['host'] == 'localhost'
    assert pg_config['password'] == 'pg_pass'
    
    # Check MySQL config
    mysql_config = loader.get_mysql_config()
    assert mysql_config['host'] == 'localhost'
    assert mysql_config['password'] == 'mysql_pass'
    
    # Check MongoDB config
    mongo_config = loader.get_mongodb_config()
    assert mongo_config['host'] == 'localhost'
    assert mongo_config['password'] == 'mongo_pass'
    
    # Check Backup config
    backup_dir = loader.get_backup_directory()
    assert backup_dir.name == 'backups'

def test_config_not_found():
    """Test error when config file is missing"""
    with pytest.raises(FileNotFoundError):
        ConfigLoader("non_existent.yaml")

def test_missing_env_password(mock_config_file, monkeypatch):
    """Test error when password env var is missing"""
    # Prevent loading correct .env from disk
    with patch('clidup.config.loader.load_dotenv'):
        monkeypatch.delenv('POSTGRES_PASSWORD', raising=False)
        loader = ConfigLoader(str(mock_config_file))
        
        with pytest.raises(ValueError, match="POSTGRES_PASSWORD environment variable not set"):
            loader.get_postgres_config()
