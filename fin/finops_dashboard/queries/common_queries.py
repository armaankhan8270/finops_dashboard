# finops_dashboard/queries/common_queries.py

from typing import Dict

# This dictionary holds SQL queries that are common across different parts
# of the FinOps dashboard. Queries are accessed by their key (e.g., "get_all_users").
# Placeholders like {query_history_table} will be replaced by the DataFetcher
# using values from src/config.py.
COMMON_SQL_QUERIES: Dict[str, str] = {
    "get_all_users": """
        SELECT DISTINCT
            USER_NAME
        FROM
            {query_history_table}
        WHERE
            USER_NAME IS NOT NULL
        ORDER BY
            USER_NAME;
    """,
    "get_available_warehouses": """
        SELECT DISTINCT
            WAREHOUSE_NAME
        FROM
            {warehouse_metering_history_table}
        WHERE
            WAREHOUSE_NAME IS NOT NULL
        ORDER BY
            WAREHOUSE_NAME;
    """,
    # Add more common queries here as your dashboard expands,
    # e.g., for database usage, storage trends, etc.
}