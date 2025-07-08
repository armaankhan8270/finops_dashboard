# finops_dashboard/src/filter_manager.py

from datetime import datetime, timedelta
from typing import Tuple, Optional, List
import streamlit as st
from snowflake.snowpark import Session
import logging

# Import constants and utilities
from src.config import DATE_RANGES, DEFAULT_DATE_RANGE_INDEX
from src.utils import handle_errors
# Import DataFetcher (we'll need it to get the list of users)
from src.data_fetcher import DataFetcher # This will cause a circular import if DataFetcher imports FilterManager.
                                         # We'll address this by passing session/querying users directly here
                                         # or by ensuring DataFetcher is only imported later for data fetching.

logger = logging.getLogger(__name__)

class FilterManager:
    """
    Manages date and user filters for the dashboard.
    Provides methods to render filter UI and return selected values.
    """

    @staticmethod
    @handle_errors
    def get_date_filter() -> Tuple[Optional[str], Optional[str]]:
        """
        Renders the date filter UI and returns selected start and end dates as 'YYYY-MM-DD' strings.
        """
        st.subheader("Time Range Selection")

        date_range_option = st.selectbox(
            "📅 Select Date Range",
            options=list(DATE_RANGES.keys()),
            index=DEFAULT_DATE_RANGE_INDEX,
            help="Choose a predefined period or 'Custom Range' for specific dates."
        )

        start_date_str: Optional[str] = None
        end_date_str: Optional[str] = None
        
        today = datetime.now().date() # Get today's date without time

        if date_range_option == "Custom Range":
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", value=today - timedelta(days=30), max_value=today)
            with col2:
                end_date = st.date_input("End Date", value=today, max_value=today)
            
            if start_date > end_date:
                st.error("Error: End date must be after start date.")
                return None, None
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d") # Include full end day
        else:
            days_back = DATE_RANGES[date_range_option]
            start_date = today - timedelta(days=days_back)
            end_date = today # End date is always today for predefined ranges

            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d") # Include full end day

        st.info(f"Data will be filtered from **{start_date_str}** to **{end_date_str}**.")
        
        return start_date_str, end_date_str

    @staticmethod
    @handle_errors
    @st.cache_data(ttl=3600) # Cache the list of users for 1 hour
    def _get_cached_users_list(session: Session) -> List[str]:
        """
        Fetches and caches the list of distinct users from Snowflake.
        This is an internal helper to avoid re-fetching the list constantly.
        """
        logger.info("Fetching distinct user list from Snowflake...")
        try:
            # Directly execute a simple query to get users for the filter
            # We avoid using DataFetcher.fetch_data here to prevent circular dependency
            # if DataFetcher itself needs FilterManager (which it doesn't, but common pattern)
            # and to keep this specific lookup simple.
            
            # Use a raw SQL query from common_queries, making sure to replace table placeholder
            from queries.common_queries import COMMON_SQL_QUERIES
            from src.config import QUERY_HISTORY_TABLE # Ensure table name is from config

            query_text = COMMON_SQL_QUERIES["get_distinct_users_30_days"].format(query_history_table=QUERY_HISTORY_TABLE)
            
            # Execute with Snowpark session
            result = session.sql(query_text).collect()
            users = sorted([row[0] for row in result if row[0]]) # Filter out None/empty strings
            logger.info(f"Fetched {len(users)} distinct users.")
            return users
        except Exception as e:
            logger.error(f"Error fetching users for filter: {e}", exc_info=True)
            st.error(f"Failed to load user list for filter: {str(e)}")
            return []

    @staticmethod
    @handle_errors
    def get_user_filter(session: Session) -> Optional[str]:
        """
        Renders the user filter UI and returns the selected user name.
        Returns None if 'All Users' is selected.
        """
        st.subheader("User Selection")
        
        # Use the cached helper to get the user list
        users = FilterManager._get_cached_users_list(session)
        
        # Add "All Users" option
        options = ["All Users"] + users

        selected_user = st.selectbox(
            "👤 Select User",
            options=options,
            index=0, # Default to "All Users"
            help="Filter data by a specific Snowflake user. 'All Users' shows aggregated data."
        )

        return None if selected_user == "All Users" else selected_user

    @staticmethod
    def get_time_and_user_filters(session: Session) -> Dict[str, Any]:
        """
        A convenience method to get all standard filters in one call.
        Returns a dictionary suitable for passing to DataFetcher.
        """
        col1, col2 = st.columns(2)
        with col1:
            start_date_str, end_date_str = FilterManager.get_date_filter()
        with col2:
            selected_user = FilterManager.get_user_filter(session)
        
        return {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "user_name": selected_user # None if "All Users"
        }