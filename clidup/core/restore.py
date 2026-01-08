"""
Restore orchestration logic

Coordinates database restore operations with decompression and user confirmation.
"""

import logging
from datetime import datetime
from pathlib import Path

from ..databases.base import DatabaseHandler
from .compression import decompress_file, is_compressed


logger = logging.getLogger("clidup")


def confirm_restore(db_name: str) -> bool:
    """
    Ask user to confirm restore operation
    
    Args:
        db_name: Database name that will be restored
        
    Returns:
        True if user confirms, False otherwise
    """
    print()
    print("=" * 70)
    print("WARNING: DESTRUCTIVE OPERATION")
    print("=" * 70)
    print(f"You are about to restore database: '{db_name}'")
    print()
    print("This will:")
    print("  - Potentially overwrite existing data in the database")
    print("  - Execute all SQL commands from the backup file")
    print()
    print("Make sure you have:")
    print("  - A backup of the current database state (if needed)")
    print("  - Verified this is the correct database and backup file")
    print("=" * 70)
    print()
    
    response = input("Type 'yes' to continue or anything else to cancel: ").strip().lower()
    return response == 'yes'


def perform_restore(
    db_handler: DatabaseHandler,
    db_name: str,
    backup_file: Path,
    skip_confirmation: bool = False
) -> None:
    """
    Perform database restore
    
    Args:
        db_handler: Database handler instance
        db_name: Database name to restore to
        backup_file: Path to backup file
        skip_confirmation: Skip user confirmation (use with caution)
        
    Raises:
        RuntimeError: If restore fails
        FileNotFoundError: If backup file doesn't exist
    """
    start_time = datetime.now()
    
    # Validate backup file exists
    if not backup_file.exists():
        raise FileNotFoundError(f"Backup file not found: {backup_file}")
    
    # Validate database tools
    db_handler.validate_tools()
    
    # Get user confirmation
    if not skip_confirmation:
        if not confirm_restore(db_name):
            logger.info("Restore cancelled by user")
            print("\nRestore cancelled.")
            return
    
    logger.info(f"=== Starting restore to '{db_name}' ===")
    logger.info(f"Backup file: {backup_file}")
    
    try:
        # Handle compressed files
        restore_file = backup_file
        temp_file = None
        
        if is_compressed(backup_file):
            logger.info("Backup file is compressed, decompressing...")
            temp_file = decompress_file(backup_file)
            restore_file = temp_file
        
        # Perform the restore
        db_handler.restore(db_name, restore_file)
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"=== Restore completed in {duration:.2f} seconds ===")
        print(f"\nRestore completed successfully in {duration:.2f} seconds")
        
        # Clean up temporary decompressed file
        if temp_file and temp_file.exists():
            temp_file.unlink()
            logger.debug(f"Cleaned up temporary file: {temp_file}")
            
    except Exception as e:
        logger.error(f"Restore failed: {str(e)}")
        
        # Clean up temporary file if it exists
        if 'temp_file' in locals() and temp_file and temp_file.exists():
            temp_file.unlink()
            logger.debug(f"Cleaned up temporary file after error")
        
        raise
