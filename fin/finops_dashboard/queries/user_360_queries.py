# finops_dashboard/queries/user_360_queries.py

from typing import Dict

# This dictionary holds SQL queries specific to the User 360 Analysis dashboard.
# Placeholders (e.g., {query_history_table}, {user_name_filter_clause})
# will be dynamically filled by the DataFetcher.
USER_360_SQL_QUERIES: Dict[str, str] = {
    "total_credit_usage": """
        SELECT
            SUM(CASE
                WHEN EXECUTION_STATUS = 'SUCCESS' THEN TOTAL_ELAPSED_TIME / 60000 * WAREHOUSE_SIZE_IN_CREDITS_PER_HR
                ELSE 0
            END) AS TOTAL_CREDITS_USED
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND QUERY_TYPE != 'UNKNOWN'
            {user_name_filter_clause}
        ;
    """,
    "total_credit_usage_previous_period": """
        SELECT
            SUM(CASE
                WHEN EXECUTION_STATUS = 'SUCCESS' THEN TOTAL_ELAPSED_TIME / 60000 * WAREHOUSE_SIZE_IN_CREDITS_PER_HR
                ELSE 0
            END) AS TOTAL_CREDITS_USED
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND QUERY_TYPE != 'UNKNOWN'
            {user_name_filter_clause}
        ;
    """,
    "daily_credit_usage_trend": """
        SELECT
            TO_DATE(START_TIME) AS QUERY_DATE,
            SUM(CASE
                WHEN EXECUTION_STATUS = 'SUCCESS' THEN TOTAL_ELAPSED_TIME / 60000 * WAREHOUSE_SIZE_IN_CREDITS_PER_HR
                ELSE 0
            END) AS DAILY_CREDITS_USED
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND QUERY_TYPE != 'UNKNOWN'
            {user_name_filter_clause}
        GROUP BY
            QUERY_DATE
        ORDER BY
            QUERY_DATE;
    """,
    "top_users_by_cost": """
        SELECT
            USER_NAME,
            SUM(CASE
                WHEN EXECUTION_STATUS = 'SUCCESS' THEN TOTAL_ELAPSED_TIME / 60000 * WAREHOUSE_SIZE_IN_CREDITS_PER_HR
                ELSE 0
            END) AS TOTAL_CREDITS_USED
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND USER_NAME IS NOT NULL
            AND QUERY_TYPE != 'UNKNOWN'
        GROUP BY
            USER_NAME
        ORDER BY
            TOTAL_CREDITS_USED DESC
        LIMIT 100; -- Limit to top 100 users for performance if there are many
    """,
    "total_queries_executed": """
        SELECT
            COUNT(QUERY_ID) AS TOTAL_QUERIES
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND QUERY_TYPE != 'UNKNOWN'
            {user_name_filter_clause}
        ;
    """,
    "avg_query_duration": """
        SELECT
            AVG(TOTAL_ELAPSED_TIME) / 1000 AS AVG_DURATION_SECONDS -- Convert ms to seconds
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND QUERY_TYPE != 'UNKNOWN'
            AND TOTAL_ELAPSED_TIME IS NOT NULL
            {user_name_filter_clause}
        ;
    """,
    "query_performance_by_hour": """
        SELECT
            EXTRACT(HOUR FROM START_TIME) AS QUERY_HOUR,
            AVG(TOTAL_ELAPSED_TIME) / 1000 AS AVG_DURATION_SECONDS, -- Convert ms to seconds
            COUNT(QUERY_ID) AS QUERY_COUNT
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND QUERY_TYPE != 'UNKNOWN'
            AND TOTAL_ELAPSED_TIME IS NOT NULL
            {user_name_filter_clause}
        GROUP BY
            QUERY_HOUR
        ORDER BY
            QUERY_HOUR;
    """,
    "query_status_summary": """
        SELECT
            EXECUTION_STATUS,
            COUNT(QUERY_ID) AS QUERY_COUNT
        FROM
            {query_history_table}
        WHERE
            START_TIME >= '{start_date}' AND START_TIME < '{end_date}'
            AND QUERY_TYPE != 'UNKNOWN'
            {user_name_filter_clause}
        GROUP BY
            EXECUTION_STATUS
        ORDER BY
            QUERY_COUNT DESC;
    """
}