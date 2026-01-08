"""
Compression utilities for backup files

Handles compression and decompression of backup files using tar.gz format.
"""

import tarfile
import logging
from pathlib import Path


logger = logging.getLogger("clidup")


def compress_file(input_file: Path) -> Path:
    """
    Compress a file using tar.gz format
    
    Args:
        input_file: Path to file to compress
        
    Returns:
        Path to compressed file (.tar.gz)
        
    Raises:
        RuntimeError: If compression fails
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Output file will be input_file.tar.gz
    output_file = input_file.with_suffix(input_file.suffix + '.tar.gz')
    
    logger.info(f"Compressing {input_file.name}...")
    
    try:
        with tarfile.open(output_file, 'w:gz') as tar:
            # Add file to archive with just the filename (no directory path)
            tar.add(input_file, arcname=input_file.name)
        
        # Get file sizes for logging
        original_size = input_file.stat().st_size
        compressed_size = output_file.stat().st_size
        ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
        
        logger.info(
            f"Compression complete: {original_size:,} bytes → {compressed_size:,} bytes "
            f"({ratio:.1f}% reduction)"
        )
        
        # Remove original file after successful compression
        input_file.unlink()
        logger.debug(f"Removed original file: {input_file}")
        
        return output_file
        
    except Exception as e:
        error_msg = f"Compression failed: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def decompress_file(input_file: Path, output_dir: Path = None) -> Path:
    """
    Decompress a tar.gz file
    
    Args:
        input_file: Path to .tar.gz file to decompress
        output_dir: Directory to extract to. If None, uses input file's directory
        
    Returns:
        Path to decompressed file
        
    Raises:
        RuntimeError: If decompression fails
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if not input_file.suffix == '.gz':
        raise ValueError(f"File is not a .gz file: {input_file}")
    
    if output_dir is None:
        output_dir = input_file.parent
    
    logger.info(f"Decompressing {input_file.name}...")
    
    try:
        with tarfile.open(input_file, 'r:gz') as tar:
            # Extract all files
            tar.extractall(path=output_dir)
            
            # Get the name of the extracted file
            # Assuming single file in archive
            members = tar.getmembers()
            if not members:
                raise RuntimeError("Archive is empty")
            
            extracted_file = output_dir / members[0].name
        
        logger.info(f"Decompression complete: {extracted_file}")
        return extracted_file
        
    except Exception as e:
        error_msg = f"Decompression failed: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def is_compressed(file_path: Path) -> bool:
    """
    Check if a file is compressed
    
    Args:
        file_path: Path to check
        
    Returns:
        True if file appears to be compressed (.tar.gz or .gz)
    """
    return file_path.suffix == '.gz' or str(file_path).endswith('.tar.gz')
