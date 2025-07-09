# finops_dashboard/src/utils.py

import logging
import streamlit as st
import functools
import os
import sys
from typing import Callable, Any, TypeVar

# Import logging configuration from config.py
from src.config import LOGGING_LEVEL, LOGGING_FORMAT

# Define a type variable for decorators
F = TypeVar('F', bound=Callable[..., Any])

# --- 1. Logging Initialization ---
def init_logging() -> None:
    """
    Initializes the logging configuration for the entire application.
    This should be called once at the very start of the app (e.g., in app.py).
    """
    # Check if a root handler already exists to prevent duplicate logging
    if not logging.getLogger().handlers:
        logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT, stream=sys.stdout)
        logging.getLogger("snowflake.snowpark").setLevel(logging.WARNING) # Reduce verbosity of Snowpark logs
        logging.getLogger("streamlit").setLevel(logging.WARNING) # Reduce verbosity of Streamlit logs
        logging.info("Logging initialized.")
    else:
        logging.debug("Logging already initialized.")

# Get a logger instance for this module
logger = logging.getLogger(__name__)

# --- 2. Error Handling Decorator ---
def handle_errors(func: F) -> F:
    """
    A decorator to gracefully handle exceptions in Streamlit functions.
    It catches exceptions, logs them, and displays a user-friendly error message
    in the Streamlit app.
    
    Args:
        func (Callable): The function to wrap.
    
    Returns:
        Callable: The wrapped function with error handling.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_message = f"An unexpected error occurred in '{func.__name__}': {e}"
            logger.exception(error_message) # Log the full traceback
            
            # Display a user-friendly error message in Streamlit
            st.error(
                f"**Oops! An error occurred.** 😅<br>"
                f"It looks like something went wrong while loading this section.<br>"
                f"We've been notified and are working on it. Please try refreshing the page.<br><br>"
                f"Details: `{e}`",
                icon="🚨",
                unsafe_allow_html=True
            )
            # You might choose to return None, an empty DataFrame, or raise a custom error
            # depending on how you want the calling code to behave on error.
            # For UI rendering functions, returning None is often suitable.
            return None 
    return wrapper

# --- 3. Environment Detection ---
def is_running_in_snowflake_env() -> bool:
    """
    Checks if the Streamlit application is currently running within a
    Snowflake environment (e.g., Streamlit in Snowflake, Native App).
    
    Returns:
        bool: True if running in Snowflake, False otherwise.
    """
    # In Snowflake, the SNOWFLAKE_WAREHOUSE environment variable is usually set.
    # Also, get_active_session() would succeed without connection parameters.
    # For now, let's rely on a common env var for simplicity.
    return os.getenv("SNOWFLAKE_WAREHOUSE") is not None and os.getenv("SNOWFLAKE_ACCOUNT") is not None