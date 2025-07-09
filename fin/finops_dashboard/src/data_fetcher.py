# finops_dashboard/src/data_fetcher.py

import streamlit as st
from snowflake.snowpark import Session, DataFrame as SnowparkDataFrame
import pandas as pd
import os
import importlib
import logging
from typing import Dict, Any, Optional

# Import configurations and utilities
from src.config import (
    QUERY_HISTORY_TABLE,
    METERING_HISTORY_TABLE,
    WAREHOUSE_METERING_HISTORY_TABLE,
    LOGIN_HISTORY_TABLE,
)
from src.utils import handle_errors

logger = logging.getLogger(__name__)

class DataFetcher:
    """
    Manages all data fetching operations from Snowflake.
    This includes loading SQL queries, executing them securely,
    and caching results for performance.
    """

    # Stores all loaded SQL queries from the 'queries' directory
    # Structure: {query_group_name: {query_key: sql_string}}
    _all_sql_queries: Dict[str, Dict[str, str]] = {}
    _queries_base_dir: str = ""

    @classmethod
    def set_queries_base_dir(cls, base_dir: str) -> None:
        """
        Sets the base directory where SQL query files are located.
        This must be called before calling load_all_queries.

        Args:
            base_dir (str): The absolute path to the 'queries' directory.
        """
        cls._queries_base_dir = base_dir
        logger.info(f"Queries base directory set to: {base_dir}")

    @classmethod
    @handle_errors
    def load_all_queries(cls) -> None:
        """
        Loads all SQL queries from .py files within the 'queries' directory.
        Each .py file is expected to contain a dictionary named e.g.,
        'COMMON_SQL_QUERIES' or 'USER_360_SQL_QUERIES'.
        """
        if not cls._queries_base_dir:
            logger.error("Queries base directory not set. Call set_queries_base_dir() first.")
            raise ValueError("Queries base directory not set.")

        logger.info(f"Loading SQL queries from {cls._queries_base_dir}...")
        cls._all_sql_queries = {} # Clear existing queries

        # Add the queries directory to sys.path temporarily to import modules
        import sys
        if cls._queries_base_dir not in sys.path:
            sys.path.insert(0, cls._queries_base_dir)

        try:
            for root, _, files in os.walk(cls._queries_base_dir):
                for file_name in files:
                    if file_name.endswith(".py") and file_name != "__init__.py":
                        module_name = file_name[:-3] # Remove .py extension
                        
                        # Dynamically import the module
                        try:
                            # Use importlib.util and importlib.machinery for more robust dynamic import
                            # This handles cases where modules might be in subdirectories or not on sys.path
                            spec = importlib.util.spec_from_file_location(module_name, os.path.join(root, file_name))
                            if spec and spec.loader:
                                module = importlib.util.module_from_spec(spec)
                                spec.loader.exec_module(module)
                                # Check for query dictionaries (e.g., COMMON_SQL_QUERIES, USER_360_SQL_QUERIES)
                                for var_name in dir(module):
                                    if var_name.endswith("_SQL_QUERIES"):
                                        queries_dict = getattr(module, var_name)
                                        if isinstance(queries_dict, dict):
                                            cls._all_sql_queries[module_name] = queries_dict
                                            logger.info(f"Loaded queries from {file_name} under group '{module_name}'.")
                                        else:
                                            logger.warning(f"Variable '{var_name}' in {file_name} is not a dictionary.")
                            else:
                                logger.warning(f"Could not load spec for module {module_name} from {file_name}.")
                        except Exception as e:
                            logger.error(f"Error loading queries from {file_name}: {e}", exc_info=True)
            if not cls._all_sql_queries:
                logger.warning("No SQL query files found or loaded from the 'queries' directory.")

        finally:
            # Clean up sys.path
            if cls._queries_base_dir in sys.path:
                sys.path.remove(cls._queries_base_dir)

        logger.info("All SQL queries loaded successfully.")

    @classmethod
    def get_query_text(cls, query_group: str, query_key: str) -> str:
        """
        Retrieves a specific SQL query string.

        Args:
            query_group (str): The name of the query file/group (e.g., "user_360_queries").
            query_key (str): The key of the specific query within that group (e.g., "total_credit_usage").

        Returns:
            str: The SQL query string.

        Raises:
            ValueError: If the query group or key is not found.
        """
        if query_group not in cls._all_sql_queries:
            logger.error(f"Query group '{query_group}' not found in loaded queries.")
            raise ValueError(f"Query group '{query_group}' not found.")
        if query_key not in cls._all_sql_queries[query_group]:
            logger.error(f"Query key '{query_key}' not found in group '{query_group}'.")
            raise ValueError(f"Query key '{query_key}' not found in group '{query_group}'.")
        return cls._all_sql_queries[query_group][query_key]

    @staticmethod
    @st.cache_data(ttl=3600, show_spinner="Fetching data from Snowflake...",
                   # Crucial: Teach Streamlit how to hash the Snowpark Session object.
                   # We tell it to ignore the Session object when generating the cache key,
                   # because its state is managed by st.cache_resource elsewhere (in app.py).
                   hash_funcs={Session: lambda _: None})
    @handle_errors
    def _execute_snowpark_query_cached(session: Session, query_text: str, query_params: Dict[str, Any]) -> pd.DataFrame:
        """
        Executes a Snowpark query and caches its result.
        This is an internal helper method used by public fetch methods.

        Args:
            session (Session): The Snowpark session object.
            query_text (str): The SQL query string with placeholders.
            query_params (Dict[str, Any]): A dictionary of parameters to
                                           fill into the query placeholders.

        Returns:
            pd.DataFrame: The query result as a Pandas DataFrame.
        """
        logger.info(f"Executing cached query: {query_text[:100]}...") # Log first 100 chars
        
        # Replace common table names from config
        formatted_query = query_text.format(
            query_history_table=QUERY_HISTORY_TABLE,
            metering_history_table=METERING_HISTORY_TABLE,
            warehouse_metering_history_table=WAREHOUSE_METERING_HISTORY_TABLE,
            login_history_table=LOGIN_HISTORY_TABLE,
            **query_params # Also unpack specific query parameters
        )
        
        # Use a proper way to handle dynamic WHERE clauses to prevent SQL injection for user-controlled inputs
        # For simplicity in this example, we assume query_params are already sanitized
        # or used in a way that doesn't allow injection (e.g., only for fixed strings).
        # For more complex dynamic WHERE clauses with user input, consider using
        # Snowpark's .filter() or .where() on a DataFrame constructed from the table.

        # For simple string replacements of filters directly into the query:
        # Example: user_name_filter_clause in query_params would be "AND USER_NAME = '...'".
        # If user_name_filter_clause is not in query_params or is empty,
        # it will simply remove the placeholder from the query.
        final_query = formatted_query
        
        try:
            # Execute the query using Snowpark
            snowpark_df: SnowparkDataFrame = session.sql(final_query).collect()
            
            # Convert Snowpark DataFrame to Pandas DataFrame
            pandas_df: pd.DataFrame = snowpark_df.to_pandas()
            logger.info(f"Query executed successfully. Fetched {len(pandas_df)} rows.")
            return pandas_df
        except Exception as e:
            logger.error(f"Error executing Snowpark query: {e}", exc_info=True)
            # Re-raise the exception or handle it specifically if _execute_snowpark_query_cached
            # is expected to return a DataFrame on error (e.g., empty DataFrame).
            # The @handle_errors decorator above will catch this and display a message.
            raise e # Re-raise to be caught by the decorator

    @classmethod
    def fetch_data(cls, session: Session, query_key_path: str, query_params: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Fetches data from Snowflake for a given query key path and parameters.
        The result is cached.

        Args:
            session (Session): The Snowpark session object.
            query_key_path (str): The path to the query (e.g., "user_360_queries.total_credit_usage").
            query_params (Dict[str, Any]): A dictionary of parameters to pass to the query.
                                           These will replace placeholders in the SQL string.

        Returns:
            Optional[pd.DataFrame]: The query result as a Pandas DataFrame, or None if an error occurred.
        """
        try:
            query_group, query_key = query_key_path.split('.')
            query_text = cls.get_query_text(query_group, query_key)
            
            return cls._execute_snowpark_query_cached(session, query_text, query_params)
        except ValueError as e:
            logger.error(f"Invalid query key path '{query_key_path}': {e}")
            st.error(f"Configuration error: Invalid query path '{query_key_path}'. Please check application configuration.", icon="🚫")
            return None
        except Exception as e:
            # The @handle_errors decorator on _execute_snowpark_query_cached should catch most errors.
            # This outer catch is for issues specific to query retrieval/setup.
            logger.error(f"Failed to fetch data for '{query_key_path}': {e}", exc_info=True)
            return None # The decorator would have shown a message

    @classmethod
    def fetch_metric_value(cls, session: Session, query_key_path: str, query_params: Dict[str, Any]) -> Optional[Any]:
        """
        Fetches a single metric value from a Snowflake query result.
        Assumes the query returns a single row and single column.

        Args:
            session (Session): The Snowpark session object.
            query_key_path (str): The path to the query (e.g., "user_360_queries.total_credit_usage").
            query_params (Dict[str, Any]): A dictionary of parameters to pass to the query.

        Returns:
            Optional[Any]: The single metric value, or None if an error occurred or no data.
        """
        df = cls.fetch_data(session, query_key_path, query_params)
        if df is not None and not df.empty:
            # Assuming the query is designed to return a single row and column
            return df.iloc[0, 0]
        logger.warning(f"No metric value found for query: {query_key_path}")
        return None