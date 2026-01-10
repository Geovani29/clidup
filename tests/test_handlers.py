import pytest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path
import subprocess

from clidup.databases.postgres import PostgresHandler
from clidup.databases.mysql import MySQLHandler
from clidup.databases.sqlite import SQLiteHandler
from clidup.databases.mongodb import MongoDBHandler

# --- PostgreSQL Tests ---

@pytest.fixture
def postgres_handler():
    config = {
        'host': 'localhost',
        'port': 5432,
        'username': 'postgres',
        'password': 'password',
        'database': 'test_db'
    }
    return PostgresHandler(config)

def test_postgres_validate_tools(postgres_handler):
    with patch('shutil.which') as mock_which:
        mock_which.return_value = '/usr/bin/pg_dump'
        with patch.object(postgres_handler, 'validate_connection', return_value=True):
            assert postgres_handler.validate_tools() is True

def test_postgres_backup(postgres_handler):
    with patch('subprocess.run') as mock_run:
        mock_run.return_value.returncode = 0
        output_file = Path('backup.sql')
        
        postgres_handler.backup('test_db', output_file)
        
        # Verify pg_dump call
        args = mock_run.call_args[0][0]
        assert args[0] == 'pg_dump'
        assert '-h' in args
        assert 'test_db' in args

def test_postgres_restore(postgres_handler):
    with patch('subprocess.run') as mock_run:
        with patch.object(postgres_handler, '_database_exists', return_value=True):
            mock_run.return_value.returncode = 0
            input_file = Path('backup.sql')
            
            postgres_handler.restore('test_db', input_file)
            
            # Verify psql call
            args = mock_run.call_args[0][0]
            assert args[0] == 'psql'
            assert '-f' in args

# --- MySQL Tests ---

@pytest.fixture
def mysql_handler():
    config = {
        'host': 'localhost',
        'port': 3306,
        'username': 'root',
        'password': 'password',
        'database': 'test_db'
    }
    return MySQLHandler(config)

def test_mysql_backup(mysql_handler):
    with patch('subprocess.run') as mock_run:
         mysql_handler.backup('test_db', Path('backup.sql'))
         args = mock_run.call_args[0][0]
         assert args[0] == 'mysqldump'
         assert '--result-file' in args

# --- SQLite Tests ---

@pytest.fixture
def sqlite_handler(tmp_path):
    db_path = tmp_path / 'test.db'
    db_path.touch()
    config = {'db_path': str(db_path)}
    return SQLiteHandler(config)

def test_sqlite_backup(sqlite_handler, tmp_path):
    output = tmp_path / 'backup.db'
    sqlite_handler.backup('sqlite', output)
    assert output.exists()

def test_sqlite_restore(sqlite_handler, tmp_path):
    backup = tmp_path / 'backup.db'
    backup.touch()
    
    with patch('shutil.copy2') as mock_copy:
        sqlite_handler.restore('sqlite', backup)
        mock_copy.assert_called_with(backup, sqlite_handler.db_path)

# --- MongoDB Tests ---

@pytest.fixture
def mongo_handler():
    config = {
        'host': 'localhost',
        'port': 27017,
        'username': 'admin',
        'password': 'password',
        'database': 'test_db'
    }
    return MongoDBHandler(config)

def test_mongo_backup(mongo_handler):
    with patch('subprocess.run') as mock_run:
        mongo_handler.backup('test_db', Path('backup.archive'))
        args = mock_run.call_args[0][0]
        assert args[0] == 'mongodump'
        assert '--archive=backup.archive' in args
