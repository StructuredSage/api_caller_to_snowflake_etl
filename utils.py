import logging
import logging.config

log_filename = f'import-process.log'

# Configuration dictionary
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'brief': {
            'format': '[%(levelname)-8s] %(message)s'
        },
        'full': {
            'format': '%(asctime)s [%(levelname)-8s] %(name)s : %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'brief',
            'level': 'INFO',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': log_filename,
            'formatter': 'full',
            'level': 'INFO',
            'backupCount': 10,
            'maxBytes': 10485760,  # 10MB
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
        'propagate': True,
    },
}

# Apply logging configuration
logging.config.dictConfig(LOGGING_CONFIG)

def log_info(log_message: str) -> None:
    logging.info(log_message)

def log_error(log_message: str, raise_message: str = None, raise_error: bool = True) -> None:
    logging.error(log_message)

    if raise_error:
        if raise_message is None:
            raise_message = log_message
        
        raise ValueError(raise_message)
    
log_info("Logging system initialized.")   