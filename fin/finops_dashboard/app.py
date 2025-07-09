# finops_dashboard/app.py

import streamlit as st
from snowflake.snowpark import Session, get_active_session
import logging
import sys
import os

# --- Important: Add src directory to Python path for module imports ---
# This ensures that Python can find modules like src.config, src.utils, etc.
# when running the app from the root directory.
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) # Go up to finops_dashboard/
src_dir = os.path.join(parent_dir, "src")
queries_dir = os.path.join(parent_dir, "queries")

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
if queries_dir not in sys.path:
    # We only need to add 'queries_dir' here for direct imports if any,
    # but DataFetcher handles loading from this path anyway.
    # It's primarily for ensuring 'src' modules are found.
    pass

# --- Import all necessary modules for app initialization and page rendering ---
from src.config import APP_TITLE, APP_DESCRIPTION, APP_ICON
from src.utils import init_logging, handle_errors, is_running_in_snowflake_env
from src.ui_elements import UIElements
from src.data_fetcher import DataFetcher
from src.pages.user_360_page import User360Page # Import your specific page modules

# --- Initialize Logging ---
init_logging()
logger = logging.getLogger(__name__)

# --- Snowpark Session Management ---
@st.cache_resource(show_spinner="Connecting to Snowflake...", ttl=3600)
@handle_errors
def get_snowpark_session() -> Session:
    """
    Establishes and caches a Snowpark Session.
    This function is cached to ensure the session is created only once
    per Streamlit user session, improving performance.
    It automatically detects if running in Snowflake environment or locally.

    Returns:
        Session: The active Snowpark session.

    Raises:
        Exception: If unable to establish a Snowflake connection.
    """
    try:
        if is_running_in_snowflake_env():
            logger.info("Running in Snowflake environment. Getting active session.")
            # When deployed as a Streamlit in Snowflake app or Native App,
            # get_active_session() automatically provides the context.
            session = get_active_session()
            st.success("Connected to Snowflake environment via active session!", icon="✅")
        else:
            logger.info("Running locally. Connecting via .streamlit/secrets.toml...")
            # For local development, use st.connection to read secrets.toml
            # The 'snowflake' key in secrets.toml corresponds to the connection name.
            conn = st.connection("snowflake")
            session = conn.session()
            st.success("Connected to Snowflake locally via secrets.toml!", icon="✅")
        
        # Verify connection by running a simple query
        session.sql("SELECT CURRENT_ACCOUNT()").collect()
        logger.info(f"Successfully connected to Snowflake account: {session.get_current_account()}")
        
        return session
    except Exception as e:
        logger.exception("Failed to connect to Snowflake!")
        st.error(
            f"**Connection Error:** Could not connect to Snowflake. 😔<br>"
            f"Please check your network connection and/or Snowflake credentials in "
            f"`.streamlit/secrets.toml` (if running locally) or your application "
            f"privileges (if deployed in Snowflake).<br><br>"
            f"Details: `{e}`",
            icon="❌",
            unsafe_allow_html=True
        )
        raise # Re-raise to stop app execution if connection fails

# --- Main Application Logic ---
@handle_errors
def main() -> None:
    """
    Main function to run the Streamlit FinOps Dashboard application.
    """
    # 1. Set global page configuration (must be first Streamlit command)
    UIElements.set_page_config()

    # 2. Inject global CSS styles
    UIElements.render_global_styles()

    # 3. Establish Snowpark Session
    session = get_snowpark_session()
    if session is None:
        return # Stop if session couldn't be established

    # 4. Load all SQL queries into DataFetcher
    # This ensures queries are ready before any page tries to fetch data.
    DataFetcher.set_queries_base_dir(queries_dir)
    DataFetcher.load_all_queries()
    if not DataFetcher._all_sql_queries:
        st.error("No SQL queries loaded. Please check your 'queries' directory and `DataFetcher.load_all_queries()`.", icon="🚫")
        return


    # 5. Sidebar Navigation
    st.sidebar.title(f"{APP_ICON} {APP_TITLE}")
    st.sidebar.markdown(f"*{APP_DESCRIPTION}*")
    st.sidebar.markdown("---")

    page_selection: str = st.sidebar.radio(
        "Navigate Dashboard",
        options=["User 360 Analysis", "Warehouse Optimization (Coming Soon)", "Cost Forecasting (Coming Soon)"],
        index=0, # Default to "User 360 Analysis"
        key="page_selector"
    )
    st.sidebar.markdown("---")

    # 6. Render selected page
    if page_selection == "User 360 Analysis":
        User360Page.render(session)
    elif page_selection == "Warehouse Optimization (Coming Soon)":
        st.info("Warehouse Optimization page is under construction. Stay tuned!", icon="🚧")
    elif page_selection == "Cost Forecasting (Coming Soon)":
        st.info("Cost Forecasting page is under construction. Exciting features are on the way!", icon="🔮")

    # Optionally display app footer or additional info
    st.sidebar.markdown("---")
    st.sidebar.info(
        "Powered by Snowflake, Streamlit & Snowpark."
        " Developed for FinOps insights.",
        icon="💡"
    )

# --- Entry Point ---
if __name__ == "__main__":
    main()