# finops_dashboard/src/pages/user_360_page.py

import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import logging
from datetime import datetime, timedelta

# Import all our custom modules
from src.utils import handle_errors
from src.ui_elements import UIElements
from src.filter_manager import FilterManager
from src.data_fetcher import DataFetcher
from src.data_processor import DataProcessor
from src.metric_builder import MetricBuilder
from src.chart_builder import ChartBuilder
from src.config import APP_TITLE # Re-import for consistency if needed, though UIElements handles app-wide title

logger = logging.getLogger(__name__)

class User360Page:
    """
    Class responsible for rendering the User 360 Dashboard page.
    """

    @staticmethod
    @handle_errors
    def render(session: Session):
        """
        Renders the entire User 360 dashboard page.

        Args:
            session (Session): The Snowpark session object.
        """
        UIElements.render_page_header("User 360 Analysis", "Deep dive into individual Snowflake user activity and cost.")

        # --- Filters Section ---
        st.sidebar.header("Dashboard Filters")
        
        # Get selected dates and user from FilterManager
        filter_params = FilterManager.get_time_and_user_filters(session)
        start_date_str = filter_params["start_date"]
        end_date_str = filter_params["end_date"]
        selected_user = filter_params["user_name"]

        if not start_date_str or not end_date_str:
            st.warning("Please select valid start and end dates to view the dashboard.")
            return

        # Prepare parameters for DataFetcher
        query_params = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "user_name": selected_user # This will be None if "All Users"
        }

        # Calculate previous period dates for delta comparison
        start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        period_length = (end_date_obj - start_date_obj).days # Excludes the end_date itself

        prev_end_date_obj = start_date_obj
        prev_start_date_obj = start_date_obj - timedelta(days=period_length)
        
        query_params_prev = {
            "prev_start_date": prev_start_date_obj.strftime("%Y-%m-%d"),
            "prev_end_date": prev_end_date_obj.strftime("%Y-%m-%d"),
            "user_name": selected_user
        }

        st.subheader("Selected Filters:")
        filter_display_col1, filter_display_col2 = st.columns([1,2])
        with filter_display_col1:
            st.info(f"**Period:** {start_date_str} to {end_date_str}")
        with filter_display_col2:
            st.info(f"**User:** {selected_user if selected_user else 'All Users'}")
        
        st.markdown("---") # Visual separator

        # --- Main Content Area ---
        st.title(f"User Analysis: {selected_user if selected_user else 'All Users'}")

        # --- Section: Overview Metrics ---
        UIElements.render_section_header("Overview Metrics", icon="📊", description="Key performance indicators for Snowflake usage.")
        
        col1, col2, col3, col4 = st.columns(4)

        with st.spinner("Fetching overview metrics..."):
            # Total Credits Used
            total_credits_df = DataFetcher.fetch_data(session, "user_360.total_credit_usage", query_params)
            total_credits = DataProcessor.fetch_metric_value(total_credits_df) # Renamed to use DataProcessor
            
            total_credits_prev_df = DataFetcher.fetch_data(session, "user_360.total_credit_usage_previous_period", query_params_prev)
            total_credits_prev = DataProcessor.fetch_metric_value(total_credits_prev_df) # Renamed to use DataProcessor

            with col1:
                MetricBuilder.build_metric_card(
                    "Total Credits Used",
                    total_credits,
                    total_credits_prev,
                    metric_type="float_number", # Credits can be float
                    higher_is_better_for_delta=False, # Lower cost is better
                    value_suffix=" credits"
                )
            
            # Total Queries Executed
            total_queries_df = DataFetcher.fetch_data(session, "user_360.total_queries_executed", query_params)
            total_queries = DataProcessor.fetch_metric_value(total_queries_df)

            # No previous period for queries count for now, but could add similar logic
            with col2:
                MetricBuilder.build_metric_card(
                    "Total Queries Executed",
                    total_queries,
                    metric_type="number",
                    higher_is_better_for_delta=True
                )

            # Average Query Duration
            avg_query_duration_df = DataFetcher.fetch_data(session, "user_360.avg_query_duration", query_params)
            avg_query_duration = DataProcessor.fetch_metric_value(avg_query_duration_df)

            with col3:
                MetricBuilder.build_metric_card(
                    "Avg. Query Duration",
                    avg_query_duration,
                    metric_type="duration_seconds",
                    higher_is_better_for_delta=False # Lower duration is better
                )

            # Placeholder for another metric
            with col4:
                st.markdown("<br>", unsafe_allow_html=True) # Spacer for alignment
                UIElements.render_info_card(
                    "Tip",
                    "Monitor credit usage and query performance to identify optimization opportunities."
                )

        st.markdown("---")

        # --- Section: Daily Credit Usage Trend ---
        UIElements.render_section_header("Daily Credit Usage Trend", icon="📈", description="See how credits are consumed over time.")
        
        with st.spinner("Loading daily credit usage trend..."):
            daily_trend_df = DataFetcher.fetch_data(session, "user_360.daily_credit_usage_trend", query_params)
            
            if daily_trend_df is not None and not daily_trend_df.empty:
                # Ensure date column is datetime for Plotly
                daily_trend_df['QUERY_DATE'] = pd.to_datetime(daily_trend_df['QUERY_DATE'])
                
                line_chart_fig = ChartBuilder.build_line_chart(
                    daily_trend_df,
                    x_col="QUERY_DATE",
                    y_col="DAILY_CREDITS_USED",
                    title="Daily Credit Consumption",
                    x_axis_title="Date",
                    y_axis_title="Credits Used"
                )
                if line_chart_fig:
                    st.plotly_chart(line_chart_fig, use_container_width=True)
            else:
                st.info("No daily credit usage data found for the selected period/user.")
        
        st.markdown("---")

        # --- Section: Top Users by Cost (if "All Users" selected) ---
        if selected_user is None: # Only show this section if "All Users" is selected
            UIElements.render_section_header("Top Users by Cost", icon="👑", description="Identify the highest credit consumers.")
            
            with st.spinner("Identifying top users by cost..."):
                top_users_df = DataFetcher.fetch_data(session, "user_360.top_users_by_cost", query_params)

                if top_users_df is not None and not top_users_df.empty:
                    # Identify high impact users using DataProcessor
                    top_users_processed_df = DataProcessor.identify_high_impact_users(
                        top_users_df, 
                        cost_column="TOTAL_CREDITS_USED", 
                        user_column="USER_NAME"
                    )

                    # Get top N and 'Others'
                    top_n_users_for_chart = DataProcessor.get_top_n_values(
                        top_users_processed_df, 
                        value_col="TOTAL_CREDITS_USED", 
                        name_col="USER_NAME", 
                        n=7 # Show top 7 users
                    )

                    col_chart, col_table = st.columns([2,1]) # Chart wider than table

                    with col_chart:
                        bar_chart_fig = ChartBuilder.build_bar_chart(
                            top_n_users_for_chart,
                            x_col="USER_NAME",
                            y_col="TOTAL_CREDITS_USED",
                            title="Top Credit Consumers",
                            x_axis_title="User Name",
                            y_axis_title="Total Credits Used",
                            color_col="PRIORITY_LEVEL" # Color bars based on priority
                        )
                        if bar_chart_fig:
                            st.plotly_chart(bar_chart_fig, use_container_width=True)
                    
                    with col_table:
                        st.markdown("##### Top Users Details:")
                        # Display users with their priority status in a nice table/dataframe
                        # Select relevant columns for display
                        display_cols = ['USER_NAME', 'TOTAL_CREDITS_USED', 'PRIORITY_LABEL', 'PRIORITY_ICON']
                        if not top_users_processed_df.empty and all(col in top_users_processed_df.columns for col in display_cols):
                            st.dataframe(
                                top_users_processed_df[[c for c in display_cols if c in top_users_processed_df.columns]].head(10), # Show top 10
                                hide_index=True,
                                column_config={
                                    "USER_NAME": st.column_config.TextColumn("User Name", width="medium"),
                                    "TOTAL_CREDITS_USED": st.column_config.NumberColumn("Credits Used", format="%.2f", help="Total Snowflake credits consumed by user."),
                                    "PRIORITY_LABEL": st.column_config.TextColumn("Priority", help="Categorization based on average cost."),
                                    "PRIORITY_ICON": st.column_config.TextColumn("Status", help="Visual indicator of priority.")
                                }
                            )
                        else:
                            st.info("No detailed user data to display.")

                else:
                    st.info("No top user data found for the selected period.")
            
            st.markdown("---")

        # --- Section: Query Performance by Hour / Query Status Breakdown ---
        UIElements.render_section_header("Query Performance & Status", icon="⚙️", description="Analyze query execution patterns and outcomes.")
        
        col_hourly, col_status = st.columns(2)

        with col_hourly:
            with st.spinner("Loading hourly query performance..."):
                hourly_perf_df = DataFetcher.fetch_data(session, "user_360.query_performance_by_hour", query_params)
                
                if hourly_perf_df is not None and not hourly_perf_df.empty:
                    # Prepare data for heatmap
                    heatmap_data = DataProcessor.pivot_for_heatmap(
                        hourly_perf_df, 
                        index_col="QUERY_HOUR", # Using QUERY_HOUR as index now
                        columns_col="QUERY_HOUR", # This is a dummy for pivot if we want simple table
                        values_col="AVG_DURATION_SECONDS"
                    )
                    # For a single metric like AVG_DURATION, a bar chart might be better
                    # Let's pivot for AVG_DURATION_SECONDS against hours explicitly for a cleaner bar chart if not heatmap
                    
                    # For "Query Performance by Hour" a bar chart for average duration is more direct
                    bar_chart_hourly_fig = ChartBuilder.build_bar_chart(
                        hourly_perf_df,
                        x_col="QUERY_HOUR",
                        y_col="AVG_DURATION_SECONDS",
                        title="Avg Query Duration by Hour",
                        x_axis_title="Hour of Day",
                        y_axis_title="Avg. Duration (Seconds)",
                        orientation='v'
                    )
                    if bar_chart_hourly_fig:
                        st.plotly_chart(bar_chart_hourly_fig, use_container_width=True)
                else:
                    st.info("No hourly query performance data found.")

        with col_status:
            with st.spinner("Loading query status breakdown..."):
                query_status_df = DataFetcher.fetch_data(session, "user_360.query_status_summary", query_params)
                
                if query_status_df is not None and not query_status_df.empty:
                    pie_chart_status_fig = ChartBuilder.build_pie_chart(
                        query_status_df,
                        names_col="EXECUTION_STATUS",
                        values_col="QUERY_COUNT",
                        title="Query Execution Status Breakdown"
                    )
                    if pie_chart_status_fig:
                        st.plotly_chart(pie_chart_status_fig, use_container_width=True)
                else:
                    st.info("No query status data found.")
        
        st.markdown("---")

        UIElements.render_info_card(
            "About This Data",
            "Data for this dashboard is sourced from Snowflake Account Usage views (QUERY_HISTORY, METERING_HISTORY, etc.) "
            "and is cached for performance. Delays in data freshness may occur due to Snowflake's Account Usage view latency."
        )