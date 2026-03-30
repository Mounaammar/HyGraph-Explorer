# hygraph_core/logging_config.py
import logging
import sys
from pathlib import Path


def setup_logging(log_file: str = "hygraph_ingestion.log", level=logging.INFO):
    """
    Configure logging for the entire application.
    Call this BEFORE importing any other hygraph modules.
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler (stdout) - colored outputs
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers (if any)
    root_logger.handlers.clear()

    # Add new handlers
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Suppress noisy third-party loggers
    logging.getLogger('psycopg').setLevel(logging.WARNING)
    logging.getLogger('psycopg_pool').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    root_logger.info("=" * 60)
    root_logger.info("Logging configured successfully")
    root_logger.info(f"Log file: {log_path.absolute()}")
    root_logger.info("=" * 60)

    return root_logger
