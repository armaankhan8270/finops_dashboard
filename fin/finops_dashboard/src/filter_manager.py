# finops_dashboard/src/filter_manager.py

import streamlit as st
from snowflake.snowpark import Session
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional

# Import DataFetcher to get dynamic filter options
from src.data_fetcher import DataFetcher
from src.utils import handle_errors # For robust UI elements

class FilterManager:
    """
    Manages user-selectable filters for the dashboard,
    typically displayed in the Streamlit sidebar.
    """

    @staticmethod
    @handle_errors
    def get_time_and_user_filters(session: Session) -> Dict[str, Any]:
        """
        Renders date and user selection filters in the Streamlit sidebar
        and returns the selected values.

        Args:
            session (Session): The Snowpark session object required by DataFetcher.

        Returns:
            Dict[str, Any]: A dictionary containing the selected filter parameters:
                            - "start_date" (str): Start date in 'YYYY-MM-DD' format.
                            - "end_date" (str): End date in 'YYYY-MM-DD' format (exclusive).
                            - "user_name" (Optional[str]): Selected user name, or None if "All Users".
        """
        st.sidebar.subheader("Date Range")

        # Set default dates for the last 30 days
        today: date = datetime.now().date()
        # Ensure end_date_default is current date + 1 day for queries that use < end_date
        # (meaning data up to the current day is included)
        end_date_default: date = today + timedelta(days=1)
        start_date_default: date = end_date_default - timedelta(days=30)

        # Date Input Widget
        date_range_selection: Optional[List[date]] = st.sidebar.date_input(
            "Select Date Range",
            value=(start_date_default, end_date_default - timedelta(days=1)), # Display range inclusive
            min_value=date(2020, 1, 1), # Data available from
            max_value=today,
            key="date_range_filter",
            help="Select the start and end dates for your analysis."
        )

        selected_start_date_str: Optional[str] = None
        selected_end_date_str: Optional[str] = None
        
        # Process the selected date range
        if date_range_selection and len(date_range_selection) == 2:
            # For SQL queries that use 'START_TIME >= {start_date} AND START_TIME < {end_date}',
            # the end_date should be the day *after* the last day you want to include.
            # st.date_input returns the selected dates inclusively.
            selected_start_date_str = date_range_selection[0].strftime("%Y-%m-%d")
            # If the user picks a range like July 1 to July 7, date_range_selection[1] is July 7.
            # We need the SQL query to go up to July 8 (exclusive) to include all of July 7.
            selected_end_date_obj = date_range_selection[1] + timedelta(days=1)
            selected_end_date_str = selected_end_date_obj.strftime("%Y-%m-%d")
        elif date_range_selection and len(date_range_selection) == 1:
            # If only one date is selected, treat it as a single day
            selected_start_date_str = date_range_selection[0].strftime("%Y-%m-%d")
            selected_end_date_str = (date_range_selection[0] + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            # Fallback if no valid date range is selected (e.g., initial state or error)
            st.sidebar.warning("Please select a valid date range.")


        st.sidebar.subheader("User Selection")

        # Fetch distinct users from Snowflake using DataFetcher
        # This will be cached by DataFetcher._execute_snowpark_query_cached
        users_df = DataFetcher.fetch_data(session, "common_queries.get_all_users", {})
        
        user_names: List[str] = ["All Users"]
        if users_df is not None and not users_df.empty:
            user_names.extend(users_df["USER_NAME"].astype(str).tolist())
        
        selected_user: str = st.sidebar.selectbox(
            "Select User",
            options=user_names,
            key="user_filter",
            help="Filter data for a specific user or view all users."
        )

        # Prepare the user filter clause for SQL queries
        user_name_filter_clause: str = ""
        user_name_for_query: Optional[str] = None
        if selected_user and selected_user != "All Users":
            user_name_filter_clause = f"AND USER_NAME = '{selected_user}'"
            user_name_for_query = selected_user

        # Return parameters to be used by DataFetcher
        return {
            "start_date": selected_start_date_str,
            "end_date": selected_end_date_str,
            "user_name": user_name_for_query, # For dashboard display logic
            "user_name_filter_clause": user_name_filter_clause # For SQL query injection
        }