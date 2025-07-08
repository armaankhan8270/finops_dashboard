# finops_dashboard/queries/common_queries.py

# This dictionary stores all common SQL queries.
# Keys are descriptive names, values are the SQL strings.
# Use f-strings or .format() if you need dynamic table names,
# but for most cases, stick to placeholders like {start_date} for QueryManager.

COMMON_SQL_QUERIES = {
    "get_distinct_users_30_days": """
        SELECT DISTINCT user_name
        FROM {query_history_table}
        WHERE start_time >= CURRENT_DATE - 30
        ORDER BY user_name;
    """
}