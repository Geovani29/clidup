"""
CLI entry point for clidup

Provides backup and restore commands for database operations.
"""

import sys
import re
import typer
from pathlib import Path
from typing import Optional
from enum import Enum

from ..config.loader import ConfigLoader
from ..databases.postgres import PostgresHandler
from ..core.backup import perform_backup
from ..core.restore import perform_restore
from ..logging.logger import setup_logger


# Create Typer app
app = typer.Typer(
    name="clidup",
    help="Professional CLI tool for database backups and restores",
    add_completion=False
)


class DatabaseType(str, Enum):
    """Supported database types"""
    postgres = "postgres"


def extract_db_name_from_filename(filename: str) -> Optional[str]:
    """
    Extract database name from backup filename using regex
    
    Format: <db_type>_<db_name>_full_<YYYY-MM-DD>_<HH-MM-SS>.sql[.tar.gz]
    
    Args:
        filename: Backup filename (without path)
        
    Returns:
        Database name if found, None otherwise
        
    Examples:
        >>> extract_db_name_from_filename("postgres_mydb_full_2026-01-07_23-45-12.sql")
        'mydb'
        >>> extract_db_name_from_filename("postgres_my_production_db_full_2026-01-07_23-45-12.sql.tar")
        'my_production_db'
    """
    # Remove common extensions
    name = filename.replace('.tar.gz', '').replace('.tar', '').replace('.sql', '')
    
    # Pattern: postgres_<db_name>_full_<date>_<time>
    # db_name can contain underscores, so we use non-greedy match (.+?)
    pattern = r'^postgres_(.+?)_full_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}(?:-\d{2})?$'
    match = re.match(pattern, name)
    
    if match:
        return match.group(1)
    return None


@app.command()
def backup(
    db: DatabaseType = typer.Option(
        ...,
        "--db",
        help="Database type (currently only 'postgres' is supported)"
    ),
    db_name: str = typer.Option(
        ...,
        "--db-name",
        help="Name of the database to backup"
    ),
    compress: bool = typer.Option(
        False,
        "--compress",
        help="Compress the backup file using tar.gz"
    ),
    config_file: Optional[str] = typer.Option(
        None,
        "--config",
        help="Path to config.yaml file (default: searches current directory)"
    )
):
    """
    Create a backup of a database
    
    Example:
        clidup backup --db postgres --db-name myapp_db --compress
    """
    try:
        # Load configuration
        config = ConfigLoader(config_file)
        
        # Setup logger
        log_file = config.get_backup_directory() / "clidup.log"
        logger = setup_logger(log_file=log_file)
        
        # Get database configuration
        if db == DatabaseType.postgres:
            db_config = config.get_postgres_config()
            db_handler = PostgresHandler(db_config)
        else:
            typer.echo(f"Error: Unsupported database type: {db}", err=True)
            raise typer.Exit(code=1)
        
        # Get backup directory
        backup_dir = config.get_backup_directory()
        
        # Perform backup
        backup_file = perform_backup(
            db_handler=db_handler,
            db_type=db.value,
            db_name=db_name,
            backup_dir=backup_dir,
            compress=compress
        )
        
        typer.echo(f"\nBackup completed successfully!")
        typer.echo(f"Backup file: {backup_file}")
        typer.echo(f"Logs: {log_file}")
        
    except FileNotFoundError as e:
        typer.echo(f"Error: File not found: {e}", err=True)
        raise typer.Exit(code=1)
    except ValueError as e:
        typer.echo(f"Error: Configuration error: {e}", err=True)
        raise typer.Exit(code=1)
    except RuntimeError as e:
        typer.echo(f"Error: Backup failed: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"Error: Unexpected error: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def restore(
    db: DatabaseType = typer.Option(
        ...,
        "--db",
        help="Database type (currently only 'postgres' is supported)"
    ),
    file: str = typer.Option(
        ...,
        "--file",
        help="Path to backup file to restore from"
    ),
    db_name: Optional[str] = typer.Option(
        None,
        "--db-name",
        help="Name of the database to restore to (if different from backup)"
    ),
    config_file: Optional[str] = typer.Option(
        None,
        "--config",
        help="Path to config.yaml file (default: searches current directory)"
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompt (use with caution)"
    )
):
    """
    Restore a database from a backup file
    
    Example:
        clidup restore --db postgres --file backups/postgres_myapp_db_full_2026-01-07_22-30.sql
    """
    try:
        # Load configuration
        config = ConfigLoader(config_file)
        
        # Setup logger
        log_file = config.get_backup_directory() / "clidup.log"
        logger = setup_logger(log_file=log_file)
        
        # Get database configuration
        if db == DatabaseType.postgres:
            db_config = config.get_postgres_config()
            db_handler = PostgresHandler(db_config)
        else:
            typer.echo(f"❌ Unsupported database type: {db}", err=True)
            raise typer.Exit(code=1)
        
        # Determine database name
        if db_name is None:
            # Try to extract from filename using regex
            backup_path = Path(file)
            db_name = extract_db_name_from_filename(backup_path.name)
            
            if db_name:
                typer.echo(f"Info: Detected database name from filename: {db_name}")
            else:
                typer.echo(
                    "Error: Could not detect database name from filename. "
                    "Please specify --db-name explicitly.\n"
                    "Expected format: postgres_<db_name>_full_<YYYY-MM-DD>_<HH-MM-SS>.sql",
                    err=True
                )
                raise typer.Exit(code=1)
        
        # Perform restore
        backup_file = Path(file)
        perform_restore(
            db_handler=db_handler,
            db_name=db_name,
            backup_file=backup_file,
            skip_confirmation=yes
        )
        
        typer.echo(f"Logs: {log_file}")
        
    except FileNotFoundError as e:
        typer.echo(f"Error: File not found: {e}", err=True)
        raise typer.Exit(code=1)
    except ValueError as e:
        typer.echo(f"Error: Configuration error: {e}", err=True)
        raise typer.Exit(code=1)
    except RuntimeError as e:
        typer.echo(f"Error: Restore failed: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"Error: Unexpected error: {e}", err=True)
        raise typer.Exit(code=1)


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version information"
    )
):
    """
    clidup - Professional CLI tool for database backups and restores
    
    Currently supports:
    - PostgreSQL (backup and restore)
    - Compression (tar.gz)
    - Configuration via YAML and environment variables
    """
    if version:
        from .. import __version__
        typer.echo(f"clidup version {__version__}")
        raise typer.Exit()


if __name__ == "__main__":
    app()
