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
from src.data_processor import DataProcessor
from src.chart_builder import ChartBuilder
from src.metric_builder import MetricBuilder

logger = logging.getLogger(__name__)

class User360Page:
    """
    Represents the 'User 360 Analysis' dashboard page.
    This class orchestrates data fetching, processing, and visualization
    for user-specific Snowflake consumption and performance.
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
            "Dive deep into individual user credit consumption, query patterns, and performance to identify optimization opportunities."
        )

        # 1. Filters (Sidebar)
        filter_params: Dict[str, Any] = FilterManager.get_time_and_user_filters(session)
        start_date: str = filter_params["start_date"]
        end_date: str = filter_params["end_date"]
        selected_user: Optional[str] = filter_params["user_name"]
        user_name_filter_clause: str = filter_params["user_name_filter_clause"]

        if not start_date or not end_date:
            st.warning("Please select a valid date range to load data.", icon="⚠️")
            return # Stop execution if dates are not valid

        # Calculate previous period for delta comparison
        start_date_dt = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)
        period_duration = (end_date_dt - start_date_dt)

        prev_end_date_dt = start_date_dt
        prev_start_date_dt = prev_end_date_dt - period_duration

        prev_start_date: str = prev_start_date_dt.strftime("%Y-%m-%d")
        prev_end_date: str = prev_end_date_dt.strftime("%Y-%m-%d")

        st.info(
            f"**Analysis Period:** `{start_date}` to `{pd.to_datetime(end_date) - pd.Timedelta(days=1):%Y-%m-%d}` "
            f"| **User:** `{selected_user if selected_user else 'All Users'}`",
            icon="📊"
        )
        
        # Display selected filters for user confirmation
        st.write(f"Analyzing data from **{start_date}** to **{pd.to_datetime(end_date) - pd.Timedelta(days=1):%Y-%m-%d}** for user: **{selected_user if selected_user else 'All Users'}**.")

        # Using st.container and st.spinner for better UI experience during loading
        with st.container():
            st.markdown("---") # Visual separator

            # 2. Key Performance Indicators (KPIs)
            UIElements.render_section_header("Key Performance Indicators", icon="📈")

            col1, col2, col3, col4 = st.columns(4)

            with st.spinner("Calculating overall credit usage..."):
                # Define query parameters for current period
                current_period_query_params = {
                    "start_date": start_date,
                    "end_date": end_date,
                    "user_name_filter_clause": user_name_filter_clause
                }
                # Define query parameters for previous period
                previous_period_query_params = {
                    "start_date": prev_start_date,
                    "end_date": prev_end_date,
                    "user_name_filter_clause": user_name_filter_clause
                }

                # Fetch current and previous period total credits
                total_credits = DataFetcher.fetch_metric_value(session, "user_360_queries.total_credit_usage", current_period_query_params)
                prev_total_credits = DataFetcher.fetch_metric_value(session, "user_360_queries.total_credit_usage_previous_period", previous_period_query_params)

                with col1:
                    MetricBuilder.build_metric_card(
                        label="Total Credits Used",
                        current_value=total_credits,
                        previous_value=prev_total_credits,
                        metric_type="float_number",
                        higher_is_better_for_delta=False # Lower credits are better
                    )

                # Fetch current and previous period total queries
                total_queries = DataFetcher.fetch_metric_value(session, "user_360_queries.total_queries_executed", current_period_query_params)
                prev_total_queries = DataFetcher.fetch_metric_value(session, "user_360_queries.total_queries_executed", previous_period_query_params)

                with col2:
                    MetricBuilder.build_metric_card(
                        label="Total Queries Executed",
                        current_value=total_queries,
                        previous_value=prev_total_queries,
                        metric_type="number",
                        higher_is_better_for_delta=True # More queries might mean more activity, usually good
                    )

                # Fetch current and previous period avg query duration
                avg_query_duration = DataFetcher.fetch_metric_value(session, "user_360_queries.avg_query_duration", current_period_query_params)
                prev_avg_query_duration = DataFetcher.fetch_metric_value(session, "user_360_queries.avg_query_duration", previous_period_query_params)

                with col3:
                    MetricBuilder.build_metric_card(
                        label="Avg Query Duration",
                        current_value=avg_query_duration,
                        previous_value=prev_avg_query_duration,
                        metric_type="duration_seconds",
                        higher_is_better_for_delta=False # Lower duration is better
                    )
                
                # Placeholder for another metric
                with col4:
                    MetricBuilder.build_metric_card(
                        label="Cost per Query (Est.)",
                        current_value=total_credits / total_queries if total_queries and total_queries > 0 else 0,
                        previous_value=prev_total_credits / prev_total_queries if prev_total_queries and prev_total_queries > 0 else 0,
                        metric_type="float_number",
                        higher_is_better_for_delta=False # Lower cost per query is better
                    )

            st.markdown("---") # Visual separator

            # 3. Credit Usage Trend
            UIElements.render_section_header("Credit Consumption Trend", icon="💸", description="Daily breakdown of credit usage over the selected period.")
            
            with st.spinner("Fetching daily credit usage trend..."):
                daily_credit_df = DataFetcher.fetch_data(session, "user_360_queries.daily_credit_usage_trend", current_period_query_params)
                
                if daily_credit_df is not None and not daily_credit_df.empty:
                    daily_credit_fig = ChartBuilder.build_line_chart(
                        df=daily_credit_df,
                        x_col="QUERY_DATE",
                        y_col="DAILY_CREDITS_USED",
                        title="Daily Credit Usage",
                        x_axis_title="Date",
                        y_axis_title="Credits Used"
                    )
                    if daily_credit_fig:
                        st.plotly_chart(daily_credit_fig, use_container_width=True)
                else:
                    st.info("No daily credit usage data available for the selected filters.")

            st.markdown("---")

            # 4. Top Users by Credit Consumption / User Impact
            UIElements.render_section_header("Top Users by Credit Consumption", icon="👤", description="Identifies users with the highest credit usage, categorized by impact.")

            with st.spinner("Analyzing top users..."):
                top_users_df = DataFetcher.fetch_data(session, "user_360_queries.top_users_by_cost", current_period_query_params)

                if top_users_df is not None and not top_users_df.empty:
                    # Process users to identify impact
                    processed_top_users_df = DataProcessor.identify_high_impact_users(
                        top_users_df,
                        cost_column="TOTAL_CREDITS_USED",
                        user_column="USER_NAME"
                    )

                    # Display priority alerts for high-impact users
                    high_impact_users = processed_top_users_df[processed_top_users_df['PRIORITY_LEVEL'] == 3]
                    if not high_impact_users.empty:
                        for _, row in high_impact_users.iterrows():
                            UIElements.render_priority_alert(
                                "High Priority",
                                f"High Credit Usage: {row['USER_NAME']} ({row['TOTAL_CREDITS_USED']:.2f} Credits)",
                                f"This user is in the top {DataProcessor.identify_high_impact_users.__defaults__[2]}% of credit consumers. Investigate query patterns."
                            )
                    
                    # Get top N for visualization (e.g., top 10 users + others)
                    chart_users_df = DataProcessor.get_top_n_values(
                        processed_top_users_df,
                        value_col="TOTAL_CREDITS_USED",
                        name_col="USER_NAME",
                        n=10
                    )

                    top_users_fig = ChartBuilder.build_bar_chart(
                        df=chart_users_df,
                        x_col="USER_NAME",
                        y_col="TOTAL_CREDITS_USED",
                        title="Top 10 Users by Credits Used",
                        x_axis_title="User Name",
                        y_axis_title="Credits Used",
                        orientation="h" # Horizontal bars for better user name readability
                        # color_col="PRIORITY_LABEL" # Can color by priority if desired, will use custom color_map
                    )
                    if top_users_fig:
                        st.plotly_chart(top_users_fig, use_container_width=True)
                else:
                    st.info("No top user data available for the selected filters.")

            st.markdown("---")

            # 5. Query Status & Performance by Hour
            col_status, col_perf = st.columns(2)

            with col_status:
                UIElements.render_section_header("Query Status Summary", icon="✅", description="Breakdown of query execution statuses (success, failed, cancelled).")
                with st.spinner("Fetching query status summary..."):
                    query_status_df = DataFetcher.fetch_data(session, "user_360_queries.query_status_summary", current_period_query_params)
                    if query_status_df is not None and not query_status_df.empty:
                        query_status_fig = ChartBuilder.build_pie_chart(
                            df=query_status_df,
                            names_col="EXECUTION_STATUS",
                            values_col="QUERY_COUNT",
                            title="Query Execution Status"
                        )
                        if query_status_fig:
                            st.plotly_chart(query_status_fig, use_container_width=True)
                    else:
                        st.info("No query status data available for the selected filters.")

            with col_perf:
                UIElements.render_section_header("Query Performance by Hour", icon="⏱️", description="Average query duration and count per hour of the day.")
                with st.spinner("Fetching query performance by hour..."):
                    query_perf_by_hour_df = DataFetcher.fetch_data(session, "user_360_queries.query_performance_by_hour", current_period_query_params)
                    if query_perf_by_hour_df is not None and not query_perf_by_hour_df.empty:
                        
                        # Prepare data for heatmap: pivot from long to wide format (hours vs metric)
                        # We need to explicitly define the index and columns for the heatmap's z_data
                        # Let's create two heatmaps: one for avg duration, one for query count
                        
                        # Avg Duration Heatmap
                        hours_of_day = list(range(24))
                        
                        # Create a pivot table for avg duration
                        duration_pivot_df = query_perf_by_hour_df.pivot_table(
                            index=pd.Series([0], name='Metric'), # Dummy index for single row heatmap
                            columns='QUERY_HOUR',
                            values='AVG_DURATION_SECONDS',
                            aggfunc='mean'
                        ).reindex(columns=hours_of_day, fill_value=0) # Ensure all hours are present

                        duration_heatmap_fig = ChartBuilder.build_heatmap(
                            df=query_perf_by_hour_df, # original df, not directly used by heatmap, but for consistency
                            x_labels=hours_of_day,
                            y_labels=["Avg Duration (s)"],
                            z_data=duration_pivot_df,
                            title="Avg Query Duration by Hour",
                            x_axis_title="Hour of Day (24-hour)",
                            y_axis_title="Metric Value",
                            colorscale='Viridis',
                            zmin=0
                        )
                        if duration_heatmap_fig:
                            st.plotly_chart(duration_heatmap_fig, use_container_width=True)

                        # Query Count Heatmap
                        query_count_pivot_df = query_perf_by_hour_df.pivot_table(
                            index=pd.Series([0], name='Metric'), # Dummy index for single row heatmap
                            columns='QUERY_HOUR',
                            values='QUERY_COUNT',
                            aggfunc='sum'
                        ).reindex(columns=hours_of_day, fill_value=0) # Ensure all hours are present

                        count_heatmap_fig = ChartBuilder.build_heatmap(
                            df=query_perf_by_hour_df,
                            x_labels=hours_of_day,
                            y_labels=["Query Count"],
                            z_data=query_count_pivot_df,
                            title="Total Queries by Hour",
                            x_axis_title="Hour of Day (24-hour)",
                            y_axis_title="Metric Value",
                            colorscale='Greens',
                            zmin=0
                        )
                        if count_heatmap_fig:
                            st.plotly_chart(count_heatmap_fig, use_container_width=True)

                    else:
                        st.info("No query performance data by hour available for the selected filters.")