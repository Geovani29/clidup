"""
Backup orchestration logic

Coordinates database backup operations with compression and logging.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..databases.base import DatabaseHandler
from .compression import compress_file


logger = logging.getLogger("clidup")


def generate_backup_filename(db_type: str, db_name: str) -> str:
    """
    Generate backup filename with timestamp
    
    Format: <db_type>_<db_name>_full_<YYYY-MM-DD>_<HH-MM>.sql
    
    Args:
        db_type: Database type (e.g., 'postgres')
        db_name: Database name
        
    Returns:
        Backup filename
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{db_type}_{db_name}_full_{timestamp}.sql"


def perform_backup(
    db_handler: DatabaseHandler,
    db_type: str,
    db_name: str,
    backup_dir: Path,
    compress: bool = False
) -> Path:
    """
    Perform database backup
    
    Args:
        db_handler: Database handler instance
        db_type: Database type (e.g., 'postgres')
        db_name: Database name to backup
        backup_dir: Directory to store backup
        compress: Whether to compress the backup
        
    Returns:
        Path to backup file (compressed or uncompressed)
        
    Raises:
        RuntimeError: If backup fails
    """
    start_time = datetime.now()
    
    # Validate database tools
    db_handler.validate_tools()
    
    # Generate backup filename
    filename = generate_backup_filename(db_type, db_name)
    backup_file = backup_dir / filename
    
    logger.info(f"=== Starting backup of '{db_name}' ===")
    logger.info(f"Backup file: {backup_file}")
    
    try:
        # Perform the backup
        db_handler.backup(db_name, backup_file)
        
        # Verify file was created
        if not backup_file.exists():
            raise RuntimeError("Backup file was not created")
        
        file_size = backup_file.stat().st_size
        logger.info(f"Backup created successfully: {file_size:,} bytes")
        
        # Compress if requested
        final_file = backup_file
        if compress:
            final_file = compress_file(backup_file)
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"=== Backup completed in {duration:.2f} seconds ===")
        logger.info(f"Final backup file: {final_file}")
        
        return final_file
        
    except Exception as e:
        logger.error(f"Backup failed: {str(e)}")
        
        # Clean up partial backup file if it exists
        if backup_file.exists():
            backup_file.unlink()
            logger.debug(f"Cleaned up partial backup file")
        
        raise
