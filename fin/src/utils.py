# finops_dashboard/src/utils.py

import logging
import streamlit as st
from typing import Any, Callable, TypeVar, ParamSpec
from functools import wraps
import os # For checking if in Snowflake env

# Type variables for better type hinting with decorators
P = ParamSpec('P') # Represents the parameters of the decorated function
R = TypeVar('R')   # Represents the return type of the decorated function

# Initialize a logger for this module
# We'll configure basic logging globally in app.py once.
logger = logging.getLogger(__name__)

def init_logging():
    """
    Initializes basic logging configuration for the application.
    This should be called once at the start of the application (e.g., in app.py).
    """
    if not logging.root.handlers: # Prevent re-initializing handlers if already set up
        logging.basicConfig(
            level=logging.INFO, # Default log level
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(), # Log to console/Streamlit output
                # You could add a FileHandler here for persistent logs in production, e.g.:
                # logging.FileHandler("finops_dashboard.log")
            ]
        )
        logger.info("Global logging initialized.")
    else:
        logger.debug("Global logging already initialized.")


def is_running_in_snowflake_env() -> bool:
    """
    Checks if the application is running within a Snowflake environment (e.g., Snowpark Streamlit app).
    This helps in adapting behavior, like session retrieval.
    """
    # This is a common heuristic. Snowflake might set specific environment variables.
    # SNOWFLAKE_WAREHOUSE and SNOWFLAKE_DATABASE are often set in a native app environment.
    # For a local Snowpark connection, get_active_session() will fail.
    return "SNOWFLAKE_WAREHOUSE" in os.environ or "SNOWFLAKE_DATABASE" in os.environ


def handle_errors(func: Callable[P, R]) -> Callable[P, Optional[R]]:
    """
    A decorator for robust error handling in Streamlit components or functions.
    It logs the error details and displays a user-friendly error message in Streamlit.

    Args:
        func (Callable): The function to wrap with error handling.

    Returns:
        Callable: The wrapped function. If an error occurs, it returns None.
    """
    @wraps(func) # Preserves the original function's metadata
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_message = f"An error occurred in '{func.__name__}': {str(e)}"
            logger.exception(error_message)  # Logs the full traceback
            st.error(f"⚠️ **Dashboard Error**: {error_message}. Please check the logs for details.")
            return None  # Return None or a sensible default to indicate failure
    return wrapper