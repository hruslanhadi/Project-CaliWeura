# =====================================================
# LOGGING CONFIGURATION - PRODUCTION
# =====================================================
# Production logging with structured JSON logging and aggregation
# Compatible with Loki/ELK stack for centralized logging

import logging.config
import os
import json
from pythonjsonlogger import jsonlogger

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'json': {
            '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': '%(timestamp)s %(level)s %(name)s %(funcName)s %(lineno)d %(message)s'
        },
        'standard': {
            'format': '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': LOG_LEVEL,
            'formatter': 'json',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': LOG_LEVEL,
            'formatter': 'json',
            'filename': '/opt/airflow/logs/airflow.log',
            'maxBytes': 104857600,  # 100MB
            'backupCount': 10
        },
        'task_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': LOG_LEVEL,
            'formatter': 'json',
            'filename': '/opt/airflow/logs/tasks.log',
            'maxBytes': 104857600,  # 100MB
            'backupCount': 10
        },
        'scheduler_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': LOG_LEVEL,
            'formatter': 'json',
            'filename': '/opt/airflow/logs/scheduler.log',
            'maxBytes': 104857600,  # 100MB
            'backupCount': 10
        },
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
            'handlers': ['console', 'task_file'],
            'propagate': False
        },
        'airflow.scheduler': {
            'level': LOG_LEVEL,
            'handlers': ['console', 'scheduler_file'],
            'propagate': False
        },
        'airflow.models.dag': {
            'level': LOG_LEVEL,
            'handlers': ['console', 'file'],
            'propagate': False
        }
    }
}

if __name__ == '__main__':
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(__name__)
    logger.info('Production logging configuration loaded')
