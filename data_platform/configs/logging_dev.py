# =====================================================
# LOGGING CONFIGURATION - DEVELOPMENT
# =====================================================
# Minimal logging setup for 16GB RAM laptop
# Simple logging to files and console

# Python logging configuration for development
import logging.config
import os

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': LOG_FORMAT
        },
        'verbose': {
            'format': '[%(asctime)s] %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': LOG_LEVEL,
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': LOG_LEVEL,
            'formatter': 'verbose',
            'filename': '/opt/airflow/logs/airflow.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5
        }
    },
    'root': {
        'level': LOG_LEVEL,
        'handlers': ['console', 'file']
    },
    'loggers': {
        'airflow': {
            'level': LOG_LEVEL,
            'handlers': ['console', 'file'],
            'propagate': False
        },
        'airflow.task': {
            'level': LOG_LEVEL,
            'handlers': ['file'],
            'propagate': False
        },
        'airflow.scheduler': {
            'level': LOG_LEVEL,
            'handlers': ['console', 'file'],
            'propagate': False
        }
    }
}

if __name__ == '__main__':
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(__name__)
    logger.info('Logging configuration loaded for development')
