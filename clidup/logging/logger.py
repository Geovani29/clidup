"""
Logging configuration for clidup

Provides centralized logging with file and console output.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str = "clidup",
    log_file: Optional[Path] = None,
    level: int = logging.INFO
) -> logging.Logger:
    """
    Set up logger with file and console handlers
    
    Args:
        name: Logger name
        log_file: Path to log file. If None, uses ./backups/clidup.log
        level: Logging level
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    
    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler (all levels)
    if log_file is None:
        log_file = Path("./backups/clidup.log")
    
    # Ensure log directory exists
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Rotating file handler: max 10MB, keep 5 backup files
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    return logger


def sanitize_log_message(message: str) -> str:
    """
    Sanitize log messages to remove potential sensitive information
    
    Args:
        message: Original message
        
    Returns:
        Sanitized message
    """
    # List of sensitive keywords to redact
    sensitive_keywords = ['password', 'passwd', 'pwd', 'secret', 'token', 'key']
    
    lower_message = message.lower()
    for keyword in sensitive_keywords:
        if keyword in lower_message:
            # Return a warning instead of the actual message
            return "[REDACTED - Message contained sensitive information]"
    
    return message
