import pytest
from clidup.databases.factory import DatabaseFactory
from clidup.databases.postgres import PostgresHandler
from clidup.databases.mysql import MySQLHandler
from clidup.databases.sqlite import SQLiteHandler
from clidup.databases.mongodb import MongoDBHandler

def test_factory_postgres():
    config = {'host': 'localhost', 'port': 5432, 'username': 'user', 'password': 'pw', 'database': 'db'}
    handler = DatabaseFactory.get_handler('postgres', config)
    assert isinstance(handler, PostgresHandler)

def test_factory_mysql():
    config = {'host': 'localhost'}
    handler = DatabaseFactory.get_handler('mysql', config)
    assert isinstance(handler, MySQLHandler)

def test_factory_sqlite():
    config = {'db_path': 'test.db'}
    handler = DatabaseFactory.get_handler('sqlite', config)
    assert isinstance(handler, SQLiteHandler)

def test_factory_mongodb():
    config = {'host': 'localhost'}
    handler = DatabaseFactory.get_handler('mongodb', config)
    assert isinstance(handler, MongoDBHandler)

def test_factory_invalid_type():
    with pytest.raises(ValueError, match="Unsupported database type"):
        DatabaseFactory.get_handler('invalid', {})
