import pytest
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock
from clidup.cli.main import app

runner = CliRunner()

def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "clidup version" in result.stdout

def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Professional CLI tool" in result.stdout

def test_backup_command_missing_args():
    result = runner.invoke(app, ["backup"])
    assert result.exit_code != 0
    assert "Missing option" in result.stdout

@pytest.fixture
def mock_config_loader(tmp_path):
    with patch('clidup.cli.main.ConfigLoader') as MockLoader:
        instance = MockLoader.return_value
        instance.get_backup_directory.return_value = tmp_path
        # Mock configs
        instance.get_postgres_config.return_value = {
            'host': 'localhost', 'port': 5432, 
            'username': 'user', 'password': 'pw', 
            'database': 'db'
        }
        yield MockLoader

@pytest.fixture
def mock_dependencies():
    with patch('clidup.cli.main.setup_logger'), \
         patch('clidup.cli.main.DatabaseFactory') as mock_factory:
         # factory.get_handler returns a mock handler
         mock_factory.get_handler.return_value = MagicMock()
         yield

def test_backup_flow(mock_config_loader, mock_dependencies):
    with patch('clidup.cli.main.perform_backup') as mock_perform:
        mock_perform.return_value = "backup.sql"
        
        result = runner.invoke(app, [
            "backup", 
            "--db", "postgres", 
            "--db-name", "test_db",
            "--config", "config.yaml"
        ])
        
        if result.exit_code != 0:
            print(f"Stdout: {result.stdout}")
        
        assert result.exit_code == 0
        assert "Backup completed successfully" in result.stdout
        mock_perform.assert_called_once()

def test_restore_flow(mock_config_loader, mock_dependencies):
    with patch('clidup.cli.main.perform_restore') as mock_perform:
        result = runner.invoke(app, [
            "restore",
            "--db", "postgres",
            "--file", "backup.sql",
            "--db-name", "test_db",
            "--yes"  # skip confirmation
        ])
        
        if result.exit_code != 0:
            print(f"Stdout: {result.stdout}")
        
        assert result.exit_code == 0
        mock_perform.assert_called_once()
