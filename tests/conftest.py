import pytest
from pathlib import Path
import os
import yaml

@pytest.fixture
def mock_config_file(tmp_path):
    """Create a temporary config.yaml file"""
    config_content = {
        'postgres': {
            'host': 'localhost',
            'port': 5432,
            'username': 'postgres',
            'database': 'test_db'
        },
        'mysql': {
            'host': 'localhost',
            'port': 3306,
            'username': 'root',
            'database': 'test_db'
        },
        'sqlite': {
            'db_path': str(tmp_path / 'test.db')
        },
        'mongodb': {
            'host': 'localhost',
            'port': 27017,
            'username': 'admin',
            'auth_database': 'admin'
        },
        'backup': {
            'directory': str(tmp_path / 'backups')
        }
    }
    
    config_file = tmp_path / 'config.yaml'
    with open(config_file, 'w') as f:
        yaml.dump(config_content, f)
        
    return config_file

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set environment variables for testing"""
    monkeypatch.setenv('POSTGRES_PASSWORD', 'pg_pass')
    monkeypatch.setenv('MYSQL_PASSWORD', 'mysql_pass')
    monkeypatch.setenv('MONGODB_PASSWORD', 'mongo_pass')
