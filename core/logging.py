import logging
import sys
from typing import List
from core.config import settings

def setup_logging():
    """Configure structured logging"""
    
    log_level = getattr(logging, settings.LOG_LEVEL, logging.INFO)
    
    # JSON formatter for structured logging
    class StructuredFormatter(logging.Formatter):
        def format(self, record):
            log_entry = {
                'timestamp': self.formatTime(record),
                'level': record.levelname,
                'logger': record.name,
                'message': record.getMessage(),
                'module': record.module,
                'line': record.lineno
            }
            
            if hasattr(record, 'request_id'):
                log_entry['request_id'] = record.request_id
            if hasattr(record, 'ticker'):
                log_entry['ticker'] = record.ticker
            if hasattr(record, 'provider'):
                log_entry['provider'] = record.provider
                
            if record.exc_info:
                log_entry['exception'] = self.formatException(record.exc_info)
                
            return str(log_entry)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Console handler with structured formatting
    console_handler = logging.StreamHandler(sys.stdout)
    if settings.LOG_FORMAT == "json":
        console_handler.setFormatter(StructuredFormatter())
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    # File handler if enabled
    if settings.LOG_ENABLE_FILE:
        file_handler = logging.FileHandler('app.log', encoding='utf-8')
        file_handler.setFormatter(StructuredFormatter())
        logger.addHandler(file_handler)

    logging.getLogger("uvicorn.access").disabled = True
    
    return logger

# Initialize logging
logger = setup_logging()
