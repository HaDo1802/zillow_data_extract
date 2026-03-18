import logging
import os


def is_running_in_airflow():
    """
    Detect if code is running within Airflow context.

    Returns:
        bool: True if running in Airflow, False otherwise
    """
    # Check for Airflow-specific environment variables
    return "airflow" in os.getcwd().lower()


def setup_logger():
    """
    Set up logging that works with both Airflow and standalone execution.

    - Always creates file handler to write to etl_log/log.txt
    - In Airflow: Adds file handler without clearing Airflow's handlers
    - Standalone: Creates both file and console handlers

    Returns:
        str: Path to log file
    """
    # Set up log file path
    project_root = os.path.dirname(os.path.dirname(__file__))
    log_dir = os.path.join(project_root, "etl_log")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "log.txt")

    # Get root logger
    root_logger = logging.getLogger()

    # Set level if not already set
    if root_logger.level == logging.NOTSET:
        root_logger.setLevel(logging.INFO)

    # Formatter for our handlers
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Check if we already added our file handler (avoid duplicates on reimport)
    existing_file_handlers = [
        h
        for h in root_logger.handlers
        if isinstance(h, logging.FileHandler) and hasattr(h, "baseFilename") and h.baseFilename == log_file
    ]

    # Add file handler if not already present
    if not existing_file_handlers:
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # In standalone mode (not Airflow), also add console handler
    if not is_running_in_airflow():
        # Check if console handler already exists
        existing_console_handlers = [
            h for h in root_logger.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]

        if not existing_console_handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

    return log_file


def get_logger(name):
    """
    Get a logger for a specific module.

    This works in both Airflow and standalone contexts:
    - In Airflow: Returns logger that uses Airflow's configuration
    - Standalone: Returns logger with custom file/console handlers

    Args:
        name: Module name (typically use __name__)

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)

    # In Airflow, don't set level - let Airflow control it
    # In standalone, ensure INFO level if not already set
    if not is_running_in_airflow() and not logger.level:
        logger.setLevel(logging.INFO)

    return logger


# Initialize logging on module import
# This is safe because:
# - In Airflow: Adds file handler to etl_log/log.txt (preserves Airflow's handlers)
# - Standalone: Sets up file + console logging
# - Always returns path to log file
LOG_FILE = setup_logger()
