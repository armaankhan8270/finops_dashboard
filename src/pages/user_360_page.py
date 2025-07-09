# finops_dashboard/src/pages/user_360_page.py

import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import logging
from typing import Dict, Any, Optional

# Import all necessary modules
from src.utils import handle_errors
from src.ui_elements import UIElements
from src.filter_manager import FilterManager
from src.data_fetcher import DataFetcher
from src.data_processor import DataProcessor # Still useful for generic processing like delta colors
from src.chart_builder import ChartBuilder
from src.metric_builder import MetricBuilder
from src.config import PRIORITY_LEVELS # Needed for mapping priority labels to config colors

logger = logging.getLogger(__name__)

class User360Page:
    """
    Represents the 'User 360 Analysis' dashboard page,
    specifically designed to integrate and visualize data from
    the new set of SQL queries provided by the user.
    """

    @staticmethod
    @handle_errors
    def render(session: Session) -> None:
        """
        Renders the entire User 360 Analysis dashboard page.

        Args:
            session (Session): The active Snowpark session.
        """
        UIElements.render_page_header(
            "User 360 Analysis",
            "Deep dive into user and role-based cost, query performance, and behavior patterns based on your custom queries."
        )

        # 1. Filters (Sidebar)
        # We will still use a start_date filter, as all your queries depend on it.
        # The end_date is kept for consistency but mainly affects previous period calculation
        # and conceptual display, as your current queries run from start_date "onwards".
        filter_params: Dict[str, Any] = FilterManager.get_time_and_user_filters(session)
        start_date: str = filter_params["start_date"]
        # end_date is primarily for the filter component and previous period calculation
        # The actual queries only use start_date for filtering.
        end_date: str = filter_params["end_date"]
        selected_user: Optional[str] = filter_params["user_name"]
        user_filter_clause: str = filter_params["user_name_filter_clause"] # Matches your '{user_filter}'

        if not start_date: # Only start_date is strictly required by your queries
            st.warning("Please select a valid start date to load data.", icon="⚠️")
            return # Stop execution if dates are not valid

        # Calculate previous period for delta comparison for relevant metrics
        # Assumes the previous period is the same duration *before* the start_date.
        start_date_dt = pd.to_datetime(start_date)
        # Using a fixed 30-day period for delta calculation if no end_date in query,
        # or you can make this configurable. Here, end_date will be used to define the period.
        period_duration = (pd.to_datetime(end_date) - start_date_dt)

        prev_end_date_dt = start_date_dt
        prev_start_date_dt = prev_end_date_dt - period_duration

        prev_start_date: str = prev_start_date_dt.strftime("%Y-%m-%d")
        prev_end_date: str = prev_end_date_dt.strftime("%Y-%m-%d")


        st.info(
            f"**Analysis Period:** From `{start_date}` (inclusive) "
            f"| **User:** `{selected_user if selected_user else 'All Users'}`",
            icon="📊"
        )
        
        st.write(f"Analyzing data from **{start_date}** onwards for user: **{selected_user if selected_user else 'All Users'}**.")
        st.caption("Note: Your custom queries filter data from the selected start date up to the present (or last available data in ACCOUNT_USAGE).")

        with st.container():
            st.markdown("---")

            # 2. Key Performance Indicators (KPIs) - Using your query outputs
            UIElements.render_section_header("Core Performance Indicators", icon="📊")

            col1, col2, col3, col4 = st.columns(4)
            col5, col6, col7, col8 = st.columns(4) # For 8 KPIs

            # Prepare common query parameters for current and previous periods
            current_period_query_params = {
                "start_date": start_date,
                "user_filter": user_filter_clause # Match your placeholder name
            }
            previous_period_query_params = {
                "start_date": prev_start_date, # Use shifted start date for previous period
                "user_filter": user_filter_clause
            }
            # For "total_users_defined" which is not time-bound, pass empty dict
            non_time_bound_params = {}

            with st.spinner("Calculating core metrics..."):
                # --- KPI 1: Total Queries Run ---
                total_queries_run = DataFetcher.fetch_metric_value(session, "user_360_queries.total_queries_run", current_period_query_params)
                prev_total_queries_run = DataFetcher.fetch_metric_value(session, "user_360_queries.total_queries_run", previous_period_query_params)
                with col1:
                    MetricBuilder.build_metric_card(
                        label="Total Queries Run",
                        current_value=total_queries_run,
                        previous_value=prev_total_queries_run,
                        metric_type="number",
                        higher_is_better_for_delta=True
                    )

                # --- KPI 2: Total Active Users ---
                total_active_users = DataFetcher.fetch_metric_value(session, "user_360_queries.total_active_users", current_period_query_params)
                prev_total_active_users = DataFetcher.fetch_metric_value(session, "user_360_queries.total_active_users", previous_period_query_params)
                with col2:
                    MetricBuilder.build_metric_card(
                        label="Total Active Users",
                        current_value=total_active_users,
                        previous_value=prev_total_active_users,
                        metric_type="number",
                        higher_is_better_for_delta=True
                    )

                # --- KPI 3: Avg Cost Per User ---
                avg_cost_per_user = DataFetcher.fetch_metric_value(session, "user_360_queries.avg_cost_per_user", current_period_query_params)
                prev_avg_cost_per_user = DataFetcher.fetch_metric_value(session, "user_360_queries.avg_cost_per_user", previous_period_query_params)
                with col3:
                    MetricBuilder.build_metric_card(
                        label="Avg Cost Per User",
                        current_value=avg_cost_per_user,
                        previous_value=prev_avg_cost_per_user,
                        metric_type="currency",
                        higher_is_better_for_delta=False
                    )

                # --- KPI 4: Avg Query Duration (Seconds) ---
                avg_query_duration = DataFetcher.fetch_metric_value(session, "user_360_queries.avg_query_duration", current_period_query_params)
                prev_avg_query_duration = DataFetcher.fetch_metric_value(session, "user_360_queries.avg_query_duration", previous_period_query_params)
                with col4:
                    MetricBuilder.build_metric_card(
                        label="Avg Query Duration",
                        current_value=avg_query_duration,
                        previous_value=prev_avg_query_duration,
                        metric_type="duration_seconds",
                        higher_is_better_for_delta=False
                    )

                # --- KPI 5: Total Users Defined (Not time-bound) ---
                total_users_defined = DataFetcher.fetch_metric_value(session, "user_360_queries.total_users_defined", non_time_bound_params)
                # No previous value for non-time-bound metrics unless explicitly queried differently
                with col5:
                    MetricBuilder.build_metric_card(
                        label="Total Users Defined",
                        current_value=total_users_defined,
                        metric_type="number"
                    )

                # --- KPI 6: Percentage High Cost Users ---
                percentage_high_cost_users = DataFetcher.fetch_metric_value(session, "user_360_queries.percentage_high_cost_users", current_period_query_params)
                # For this one, no previous period delta is directly calculated in your query.
                # If you need a delta, you'd need a separate query for prev period.
                with col6:
                    MetricBuilder.build_metric_card(
                        label="% High Cost Users",
                        current_value=percentage_high_cost_users,
                        metric_type="percentage",
                        higher_is_better_for_delta=False
                    )
                
                # --- KPI 7: High Cost Users Count ---
                high_cost_users_count = DataFetcher.fetch_metric_value(session, "user_360_queries.high_cost_users_count", current_period_query_params)
                # Similar to above, no direct prev period delta from your query.
                with col7:
                    MetricBuilder.build_metric_card(
                        label="High Cost Users Count",
                        current_value=high_cost_users_count,
                        metric_type="number",
                        higher_is_better_for_delta=False
                    )

                # --- KPI 8: Failed Queries Percentage ---
                failed_queries_percentage = DataFetcher.fetch_metric_value(session, "user_360_queries.failed_queries_percentage", current_period_query_params)
                # Similar to above, no direct prev period delta from your query.
                with col8:
                    MetricBuilder.build_metric_card(
                        label="Failed Queries %",
                        current_value=failed_queries_percentage,
                        metric_type="percentage",
                        higher_is_better_for_delta=False
                    )

            st.markdown("---")

            # 3. Cost by User and Role (Top N visualization)
            UIElements.render_section_header("Top Costs by User & Role", icon="💰", description="Top 10 users and roles by cost in USD.")
            with st.spinner("Fetching cost by user and role..."):
                cost_by_user_role_df = DataFetcher.fetch_data(session, "user_360_queries.cost_by_user_and_role", current_period_query_params)

                if cost_by_user_role_df is not None and not cost_by_user_role_df.empty:
                    # Your query already returns 'name', 'cost_usd', 'type'.
                    # Let's create a combined bar chart for users and roles.
                    cost_by_user_role_fig = ChartBuilder.build_bar_chart(
                        df=cost_by_user_role_df,
                        x_col="NAME",
                        y_col="COST_USD",
                        title="Top 10 Users & Roles by Cost",
                        x_axis_title="User/Role Name",
                        y_axis_title="Cost (USD)",
                        orientation="h",
                        color_col="TYPE" # Differentiate bars by 'User' or 'Role'
                    )
                    if cost_by_user_role_fig:
                        st.plotly_chart(cost_by_user_role_fig, use_container_width=True)
                else:
                    st.info("No cost by user and role data available for the selected filters.")

            st.markdown("---")

            # 4. User Priority & Optimization Opportunities (Tables/Details)
            col_priority, col_bottleneck = st.columns([1, 2])

            with col_priority:
                UIElements.render_section_header("User Cost Priority", icon="🚨", description="Users categorized by their cost risk based on average spend.")
                with st.spinner("Analyzing user cost priorities..."):
                    # This fetches data using the 'cost_by_user_priority' query.
                    user_priority_df = DataFetcher.fetch_data(session, "user_360_queries.cost_by_user_priority", current_period_query_params)

                    if user_priority_df is not None and not user_priority_df.empty:
                        # Display alerts for high-priority users
                        # Your query provides 'PRIORITY_LEVEL' directly. Map it to our config.
                        for _, row in user_priority_df.iterrows():
                            # Map query's string to PRIORITY_LEVELS keys (or default)
                            # Assuming your query output 'Critical Cost Risk 🔴', 'High Cost Exposure 🟠', etc.
                            # We need to map these to actual keys in PRIORITY_LEVELS if they don't match exactly.
                            # Or update PRIORITY_LEVELS to include these exact strings.
                            # For simplicity, let's try to match based on the core meaning.
                            priority_key_mapping = {
                                'Critical Cost Risk 🔴': "High Priority",
                                'High Cost Exposure 🟠': "Medium Priority",
                                'Above Average Spend 🟡': "Medium Priority", # Could be 'Low Priority' depending on desired emphasis
                                'Optimized Usage 🟢': "Normal"
                            }
                            mapped_key = priority_key_mapping.get(row['PRIORITY_LEVEL'], "N/A")

                            UIElements.render_priority_alert(
                                mapped_key,
                                f"User: {row['USER_NAME']} | Cost: ${row['TOTAL_COST_USD']:.2f}",
                                f"Queries: {row['QUERY_COUNT']}, Avg Duration: {row['AVG_DURATION_SEC']:.2f}s, Failed: {row['FAILED_QUERIES']}. Status: {row['PRIORITY_LEVEL']}"
                            )
                        
                        st.markdown("---")
                        st.subheader("Detailed User Cost Breakdown")
                        # Display the DataFrame as an interactive table
                        st.dataframe(user_priority_df[[
                            "USER_NAME", "TOTAL_COST_USD", "QUERY_COUNT", "AVG_DURATION_SEC", "FAILED_QUERIES", "PRIORITY_LEVEL"
                        ]], use_container_width=True)

                    else:
                        st.info("No user cost priority data available for the selected filters.")

            with col_bottleneck:
                UIElements.render_section_header("Query Performance Bottlenecks", icon="🐢", description="Identifies queries that are slow, failing, or inefficient and suggests actions.")
                with st.spinner("Analyzing query performance bottlenecks..."):
                    # This fetches data using the 'query_performance_bottlenecks' query.
                    bottleneck_df = DataFetcher.fetch_data(session, "user_360_queries.query_performance_bottlenecks", current_period_query_params)

                    if bottleneck_df is not None and not bottleneck_df.empty:
                        # Display the DataFrame as an interactive table
                        st.dataframe(bottleneck_df[[
                            "USER_NAME", "WAREHOUSE_NAME", "QUERY_TYPE", "QUERY_COUNT",
                            "AVG_DURATION_SEC", "MAX_DURATION_SEC", "SLOW_QUERIES",
                            "FAILED_QUERIES", "SLOW_QUERY_PERCENTAGE", "PERFORMANCE_STATUS", "RECOMMENDED_ACTION"
                        ]], use_container_width=True)
                    else:
                        st.info("No query performance bottleneck data available for the selected filters.")

            st.markdown("---")

            # 5. User Behavior Patterns (Heatmap/Charts)
            UIElements.render_section_header("User Behavior Patterns", icon="🚶", description="Hourly query activity and average duration for top users.")

            with st.spinner("Analyzing user behavior patterns..."):
                # This fetches data using the 'user_behavior_patterns' query.
                user_behavior_df = DataFetcher.fetch_data(session, "user_360_queries.user_behavior_patterns", current_period_query_params)

                if user_behavior_df is not None and not user_behavior_df.empty:
                    # Pivot data for heatmap: User vs. Hour for Total Queries
                    pivot_queries_df = user_behavior_df.pivot_table(
                        index='USER_NAME',
                        columns='HOUR_OF_DAY',
                        values='TOTAL_QUERIES',
                        fill_value=0 # Fill hours with no queries as 0
                    )
                    # Ensure all 24 hours are columns, even if no data for some
                    all_hours = list(range(24))
                    pivot_queries_df = pivot_queries_df.reindex(columns=all_hours, fill_value=0)

                    # Pivot data for heatmap: User vs. Hour for Avg Duration
                    pivot_duration_df = user_behavior_df.pivot_table(
                        index='USER_NAME',
                        columns='HOUR_OF_DAY',
                        values='AVG_DURATION_SEC',
                        fill_value=0 # Fill hours with no queries as 0 duration
                    )
                    pivot_duration_df = pivot_duration_df.reindex(columns=all_hours, fill_value=0)


                    col_heatmap_queries, col_heatmap_duration = st.columns(2)

                    with col_heatmap_queries:
                        queries_heatmap_fig = ChartBuilder.build_heatmap(
                            df=user_behavior_df, # original df for consistency, z_data is the pivot
                            x_labels=pivot_queries_df.columns.tolist(),
                            y_labels=pivot_queries_df.index.tolist(),
                            z_data=pivot_queries_df,
                            title="Total Queries by User and Hour",
                            x_axis_title="Hour of Day",
                            y_axis_title="User Name",
                            colorscale='Greens',
                            zmin=0
                        )
                        if queries_heatmap_fig:
                            st.plotly_chart(queries_heatmap_fig, use_container_width=True)

                    with col_heatmap_duration:
                        duration_heatmap_fig = ChartBuilder.build_heatmap(
                            df=user_behavior_df, # original df for consistency, z_data is the pivot
                            x_labels=pivot_duration_df.columns.tolist(),
                            y_labels=pivot_duration_df.index.tolist(),
                            z_data=pivot_duration_df,
                            title="Avg Query Duration (s) by User and Hour",
                            x_axis_title="Hour of Day",
                            y_axis_title="User Name",
                            colorscale='Plasma',
                            zmin=0
                        )
                        if duration_heatmap_fig:
                            st.plotly_chart(duration_heatmap_fig, use_container_width=True)
                else:
                    st.info("No user behavior pattern data available for the selected filters.")

            st.markdown("---")

            # 6. Optimization Opportunities (Detailed Table)
            UIElements.render_section_header("Identified Optimization Opportunities", icon="💡", description="Actionable insights for cost reduction and performance improvement.")

            with st.spinner("Identifying optimization opportunities..."):
                # This fetches data using the 'optimization_opportunities' query.
                optim_opportunities_df = DataFetcher.fetch_data(session, "user_360_queries.optimization_opportunities", current_period_query_params)

                if optim_opportunities_df is not None and not optim_opportunities_df.empty:
                    # Display the DataFrame as an interactive table
                    st.dataframe(optim_opportunities_df[[
                        "USER_NAME", "WAREHOUSE_NAME", "TOTAL_QUERIES", "LONG_QUERIES",
                        "FAILED_QUERIES", "HIGH_SCAN_QUERIES", "TOTAL_COST_USD",
                        "AVG_DURATION_SEC", "LONG_QUERY_PERCENTAGE", "FAILURE_RATE",
                        "OPTIMIZATION_PRIORITY", "RECOMMENDED_ACTION"
                    ]], use_container_width=True)
                else:
                    st.info("No optimization opportunities identified for the selected filters.")
