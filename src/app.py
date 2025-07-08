# finops_dashboard/app.py

import streamlit as st
import logging
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
import sys
import os

# Ensure the 'src' directory is in the Python path
# This is crucial for importing modules like src.utils, src.pages.user_360_page etc.
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(current_dir, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)
    logging.getLogger(__name__).info(f"Added {src_path} to sys.path.")

# Import modules from our project
from src.utils import init_logging, handle_errors, is_running_in_snowflake_env
from src.ui_elements import UIElements
from src.data_fetcher import DataFetcher
# Import specific page render functions
from src.pages.user_360_page import User360Page

# Initialize logging early
init_logging()
logger = logging.getLogger(__name__)

# --- Snowpark Session Management ---
@st.cache_resource
def get_snowpark_session() -> Session:
    """
    Establishes and caches the Snowpark session.
    Checks if running in Snowflake environment or attempts local connection.
    """
    try:
        if is_running_in_snowflake_env():
            logger.info("Running in Snowflake environment. Getting active session.")
            # Get the current Snowpark session if running as a Snowflake Native App or Streamlit in Snowflake
            session = get_active_session()
            st.success("Connected to Snowflake via active session! ❄️")
            return session
        else:
            logger.info("Not running in Snowflake environment. Attempting local connection via config.")
            # Local development: Replace with your actual connection details or SnowSQL config file
            # For security, avoid hardcoding credentials directly here in production.
            # Use environment variables, Streamlit secrets, or connection.json/toml.
            
            # Example using connection.toml (recommended for local dev)
            # You would need a .streamlit/connections.toml file like:
            # [snowflake]
            # account = "YOUR_ACCOUNT"
            # user = "YOUR_USER"
            # password = "YOUR_PASSWORD"
            # role = "YOUR_ROLE"
            # warehouse = "YOUR_WAREHOUSE"
            # database = "YOUR_DATABASE"
            # schema = "YOUR_SCHEMA"
            
            # If you are NOT using connections.toml and prefer direct code (less secure for prod):
            # from snowflake.snowpark import Session
            # connection_parameters = {
            #     "account": st.secrets["SNOWFLAKE_ACCOUNT"],
            #     "user": st.secrets["SNOWFLAKE_USER"],
            #     "password": st.secrets["SNOWFLAKE_PASSWORD"],
            #     "role": st.secrets["SNOWFLAKE_ROLE"],
            #     "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
            #     "database": st.secrets["SNOWFLAKE_DATABASE"],
            #     "schema": st.secrets["SNOWFLAKE_SCHEMA"]
            # }
            # session = Session.builder.configs(connection_parameters).create()

            # Using Streamlit's built-in connection API (best practice for local + deploy)
            # Requires a [connections.snowflake] section in .streamlit/secrets.toml or connections.toml
            session = st.connection("snowflake").session()
            st.success("Successfully connected to Snowflake locally! 🚀")
            return session

    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}", exc_info=True)
        st.error(
            f"❌ **Connection Error**: Could not connect to Snowflake. "
            f"Please check your connection settings or network. Details: {e}"
        )
        st.stop() # Stop the app if connection fails
        return None # Should not be reached due to st.stop()

# --- Main Application Logic ---
@handle_errors # Apply error handling to the main app function
def main():
    """
    Main function to run the Streamlit dashboard application.
    """
    # Set Streamlit page configuration (must be first Streamlit command)
    UIElements.set_page_config()
    
    # Inject global CSS styles
    UIElements.render_global_styles()

    # Load all SQL queries once at startup
    DataFetcher.set_queries_base_dir(os.path.join(current_dir, "queries"))
    DataFetcher.load_all_queries()

    st.sidebar.title("FinOps Dashboard")
    st.sidebar.markdown("---")

    # Page Navigation
    pages = {
        "User 360 Analysis": User360Page,
        # Add more pages here as you develop them
        # "Warehouse Optimization": WarehouseOptimizationPage,
        # "Cost Forecasting": CostForecastingPage,
    }

    selected_page_name = st.sidebar.radio("Go to", list(pages.keys()))
    selected_page = pages[selected_page_name]

    st.sidebar.markdown("---")
    st.sidebar.info(
        "**💡 Tip:** Use the filters on the sidebar to adjust the data displayed."
    )

    # Get Snowpark Session (cached)
    session = get_snowpark_session()

    if session:
        # Render the selected page
        logger.info(f"Rendering page: {selected_page_name}")
        selected_page.render(session)
    else:
        st.error("Snowflake session not available. Please check connection.")
        logger.critical("Snowflake session is None, stopping app.")

# Run the main application
if __name__ == "__main__":
    main()