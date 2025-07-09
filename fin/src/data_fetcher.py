# finops_dashboard/src/data_fetcher.py

import logging
import os
import importlib.util
from typing import Any, Dict, Optional, Union
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

# Import utilities and configuration
from src.utils import handle_errors, is_running_in_snowflake_env
from src.config import QUERY_HISTORY_TABLE, METERING_HISTORY_TABLE, LOGIN_HISTORY_TABLE, WAREHOUSE_METERING_HISTORY_TABLE

logger = logging.getLogger(__name__)

class DataFetcher:
    """
    Manages loading, preparing, and executing Snowflake queries with caching.
    This is the central point for all database interactions.
    """
    _all_queries: Dict[str, Dict[str, str]] = {}
    _queries_loaded = False
    _queries_base_dir: str = "" # To store the base path of the 'queries' directory

    @classmethod
    def set_queries_base_dir(cls, path: str):
        """Sets the base path where query Python files are located."""
        cls._queries_base_dir = path
        logger.debug(f"Query base directory set to: {cls._queries_base_dir}")

    @classmethod
    def _load_queries_from_file(cls, filepath: str, module_name: str) -> None:
        """Helper to load queries from a single Python file."""
        try:
            spec = importlib.util.spec_from_file_location(module_name, filepath)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Look for a dictionary ending with _SQL_QUERIES in the module
                for attr_name in dir(module):
                    if attr_name.endswith("_SQL_QUERIES") and isinstance(getattr(module, attr_name), dict):
                        # Store queries under a namespace based on the filename (e.g., 'user_360')
                        namespace = os.path.splitext(os.path.basename(filepath))[0].replace("_queries", "")
                        cls._all_queries[namespace] = getattr(module, attr_name)
                        logger.info(f"Loaded queries from '{filepath}' under namespace '{namespace}'.")
                        return
                logger.warning(f"No SQL queries dictionary found in {filepath}. Expected a dict ending with '_SQL_QUERIES'.")
            else:
                logger.error(f"Could not load module spec or loader for {filepath}")
        except Exception as e:
            logger.error(f"Failed to load queries from {filepath}: {e}", exc_info=True)
            st.error(f"Error loading query file: {filepath}")

    @classmethod
    def load_all_queries(cls):
        """
        Loads all SQL queries from Python files in the configured queries directory.
        This should be called once at application startup.
        """
        if cls._queries_loaded:
            logger.info("Queries already loaded. Skipping re-load.")
            return

        if not cls._queries_base_dir or not os.path.exists(cls._queries_base_dir):
            error_msg = f"Queries base directory not set or found: {cls._queries_base_dir}"
            logger.error(error_msg)
            st.error(f"Configuration Error: {error_msg}. Please set the correct path to your 'queries' folder.")
            return

        for filename in os.listdir(cls._queries_base_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                filepath = os.path.join(cls._queries_base_dir, filename)
                module_name = os.path.splitext(filename)[0]
                cls._load_queries_from_file(filepath, module_name)

        cls._queries_loaded = True
        total_queries = sum(len(ns_queries) for ns_queries in cls._all_queries.values())
        logger.info(f"Loaded {total_queries} SQL queries from {len(cls._all_queries)} namespaces.")

    @classmethod
    def get_query_text(cls, query_key: str) -> Optional[str]:
        """
        Retrieves a SQL query string by its hierarchical key (e.g., "user_360.total_credit_usage").
        """
        if not cls._queries_loaded:
            cls.load_all_queries() # Attempt to load if not already loaded (useful for local dev)
            if not cls._queries_loaded: # If still not loaded, something is wrong
                return None

        parts = query_key.split('.')
        if len(parts) != 2:
            logger.error(f"Invalid query key format: '{query_key}'. Expected 'namespace.query_name'.")
            return None
        
        namespace, query_name = parts
        
        namespace_queries = cls._all_queries.get(namespace)
        if not namespace_queries:
            logger.warning(f"Query namespace '{namespace}' not found for key '{query_key}'.")
            return None
        
        query_text = namespace_queries.get(query_name)
        if query_text is None:
            logger.warning(f"Query '{query_name}' not found in namespace '{namespace}'. Full key: '{query_key}'.")
        return query_text

    @staticmethod
    @st.cache_data(ttl=3600, show_spinner="Fetching data from Snowflake...") # Cache for 1 hour
    @handle_errors # Use the utility decorator for broader error handling
    def _execute_snowpark_query_cached(
        session: Session, 
        query_text: str, 
        params: Optional[Dict[str, Any]] = None,
        query_key_for_logging: str = "unknown_query" # For better error messages
    ) -> pd.DataFrame:
        """
        Internal method to execute a Snowpark query and cache its result.
        Handles dynamic query construction and parameterized execution for security.
        """
        logger.info(f"Executing query '{query_key_for_logging}' (cached): {query_text[:100].replace('\n', ' ')}...")
        
        # Start with the base query text
        final_sql = query_text
        bind_params: List[Any] = [] # For Snowpark's `binds` parameter

        # 1. Handle common table placeholders
        # These are direct string replacements, assuming they come from trusted config.
        final_sql = final_sql.replace("{query_history_table}", QUERY_HISTORY_TABLE)
        final_sql = final_sql.replace("{metering_history_table}", METERING_HISTORY_TABLE)
        final_sql = final_sql.replace("{login_history_table}", LOGIN_HISTORY_TABLE)
        final_sql = final_sql.replace("{warehouse_metering_history_table}", WAREHOUSE_METERING_HISTORY_TABLE)
        
        # 2. Handle date placeholders (e.g., '{start_date}', '{end_date}')
        # These are also direct string replacements, wrapped in quotes for SQL strings.
        # Dates come from controlled Streamlit date inputs, so this is generally safe.
        if params:
            for date_param_key in ["start_date", "end_date", "prev_start_date", "prev_end_date"]:
                if date_param_key in params and params[date_param_key] is not None:
                    # Ensure the date is formatted as 'YYYY-MM-DD' before inserting
                    formatted_date = params[date_param_key] # Expecting string already from FilterManager
                    final_sql = final_sql.replace(f"'{'{'}{date_param_key}{'}'}'", f"'{formatted_date}'")
                    # Note: The query should have {start_date} as '{start_date}' for this to work.
                    # Or, adjust: final_sql = final_sql.replace("{" + date_param_key + "}", f"'{params[date_param_key]}'")
                    # The latter is safer if you use {start_date} without quotes in the SQL.
                    # Let's assume the SQL has '{start_date}' as per user_360_queries.py
        
            # 3. Handle specific parameterized clauses (e.g., user_name)
            # This is where we prevent SQL injection for dynamic, user-controlled strings.
            # We'll replace the `{user_name_filter_clause}` placeholder in the SQL
            # with `AND user_name = ?` and add the actual value to `bind_params`.
            user_filter_clause = ""
            if 'user_name' in params and params['user_name'] is not None:
                user_filter_clause = "AND user_name = ?"
                bind_params.append(params['user_name'])
            
            final_sql = final_sql.replace("{user_name_filter_clause}", user_filter_clause)
            
            # Add more dynamic filter clauses here as needed for other dimensions
            # e.g., warehouse, database, etc., always using '?' and adding to bind_params.


        try:
            # Execute the prepared SQL with parameters
            snowpark_df = session.sql(final_sql, binds=bind_params)
            
            # Convert to pandas DataFrame for Streamlit and Plotly compatibility
            df = snowpark_df.to_pandas()

            logger.info(f"Query '{query_key_for_logging}' executed successfully. Rows: {len(df)}")
            return df
        except SnowparkSQLException as e:
            error_detail = f"SQL State: {e.sqlstate}, Error Code: {e.error_code}, Message: {e.message}"
            logger.error(f"Snowpark SQL Error for '{query_key_for_logging}': {error_detail}\nQuery: {final_sql}", exc_info=True)
            st.error(f"🚨 **Database Error** for '{query_key_for_logging}': <br>_{e.message}_", unsafe_allow_html=True)
            return pd.DataFrame() # Return empty DataFrame on SQL error
        except Exception as e:
            logger.error(f"Unexpected error executing query '{query_key_for_logging}': {e}", exc_info=True)
            st.error(f"❌ **An unexpected error occurred** while fetching data for '{query_key_for_logging}'. <br>_{e}_", unsafe_allow_html=True)
            return pd.DataFrame() # Return empty DataFrame on generic error

    @classmethod
    def fetch_data(
        cls, 
        session: Session, 
        query_key: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Public method to fetch data using a stored query key and parameters.
        Returns a pandas DataFrame.
        """
        query_text = cls.get_query_text(query_key)
        if not query_text:
            st.error(f"Failed to retrieve query text for '{query_key}'. Data cannot be fetched.")
            return pd.DataFrame()

        # Call the cached execution method
        df = cls._execute_snowpark_query_cached(session, query_text, params, query_key_for_logging=query_key)
        return df

    @classmethod
    def fetch_metric_value(
        cls, 
        session: Session, 
        query_key: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """
        Fetches data for a metric (expected to return a single value).
        Returns the first value from the first row/column.
        """
        df = cls.fetch_data(session, query_key, params)
        if not df.empty and len(df.columns) > 0:
            return df.iloc[0, 0]
        logger.warning(f"No data or empty DataFrame returned for metric query: {query_key}")
        return None