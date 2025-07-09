# finops_dashboard/queries/user_360_queries.py

# This dictionary stores all SQL queries for the User 360 dashboard.
# Keys are descriptive names, values are the SQL strings.
# Placeholders like {start_date}, {end_date}, {user_name_filter_clause}
# will be dynamically replaced by the data_fetcher module.

USER_360_SQL_QUERIES = {
    "total_credit_usage": """
        SELECT
            SUM(credits_used) AS total_credits_used
        FROM
            {query_history_table}
        WHERE
            start_time >= '{start_date}' AND start_time < '{end_date}'
            {user_name_filter_clause} -- This placeholder will be replaced by data_fetcher
    """,

    "total_credit_usage_previous_period": """
        SELECT
            SUM(credits_used) AS total_credits_used_previous_period
        FROM
            {query_history_table}
        WHERE
            start_time >= '{prev_start_date}' AND start_time < '{prev_end_date}'
            {user_name_filter_clause}
    """,
    
    "daily_credit_usage_trend": """
        SELECT
            CAST(start_time AS DATE) AS query_date,
            SUM(credits_used) AS daily_credits_used
        FROM
            {query_history_table}
        WHERE
            start_time >= '{start_date}' AND start_time < '{end_date}'
            {user_name_filter_clause}
        GROUP BY
            query_date
        ORDER BY
            query_date;
    """,

    "total_queries_executed": """
        SELECT
            COUNT(*) AS total_queries
        FROM
            {query_history_table}
        WHERE
            start_time >= '{start_date}' AND start_time < '{end_date}'
            {user_name_filter_clause}
    """,

    "avg_query_duration": """
        SELECT
            AVG(total_elapsed_time / 1000.0) AS avg_duration_seconds -- Convert milliseconds to seconds
        FROM
            {query_history_table}
        WHERE
            start_time >= '{start_date}' AND start_time < '{end_date}'
            {user_name_filter_clause}
            AND total_elapsed_time > 0 -- Exclude queries with 0 duration
    """,

    "top_users_by_cost": """
        SELECT
            user_name,
            SUM(credits_used) AS total_credits_used
        FROM
            {query_history_table}
        WHERE
            start_time >= '{start_date}' AND start_time < '{end_date}'
        GROUP BY
            user_name
        ORDER BY
            total_credits_used DESC;
    """,

    "query_performance_by_hour": """
        SELECT
            HOUR(start_time) AS query_hour,
            COUNT(*) AS total_queries,
            AVG(total_elapsed_time / 1000.0) AS avg_duration_seconds,
            SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_queries,
            SUM(CASE WHEN execution_status != 'SUCCESS' THEN 1 ELSE 0 END) AS failed_queries
        FROM
            {query_history_table}
        WHERE
            start_time >= '{start_date}' AND start_time < '{end_date}'
            {user_name_filter_clause}
        GROUP BY
            query_hour
        ORDER BY
            query_hour;
    """,
    
    "query_status_summary": """
        SELECT
            execution_status,
            COUNT(*) AS query_count
        FROM
            {query_history_table}
        WHERE
            start_time >= '{start_date}' AND start_time < '{end_date}'
            {user_name_filter_clause}
        GROUP BY
            execution_status
        ORDER BY
            query_count DESC;
    """
}