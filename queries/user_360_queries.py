# finops_dashboard/queries/user_360_queries.py

from typing import Dict

# This dictionary holds SQL queries specific to the User 360 Analysis dashboard.
# It uses the exact queries you provided.
USER_360_SQL_QUERIES: Dict[str, str] = {
    # Core Metrics - 8 Key Performance Indicators
    "cost_by_user_and_role": """
        WITH warehouse_rates AS (
            SELECT * FROM VALUES
                ('X-Small', 1), ('Small', 2), ('Medium', 4), ('Large', 8),
                ('X-Large', 16), ('2X-Large', 32), ('3X-Large', 64), ('4X-Large', 128)
            AS warehouse_size(size, credits_per_hour)
        ),
        user_costs AS (
            SELECT
                qh.user_name AS name,
                ROUND(SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0), 2) AS cost_usd,
                'User' AS type
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.warehouse_name IS NOT NULL
            AND qh.user_name IS NOT NULL
            {user_filter}
            GROUP BY qh.user_name
        ),
        role_costs AS (
            SELECT
                qh.role_name AS name,
                ROUND(SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0), 2) AS cost_usd,
                'Role' AS type
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.warehouse_name IS NOT NULL
            AND qh.role_name IS NOT NULL
            {user_filter}
            GROUP BY qh.role_name
        )
        SELECT name, cost_usd, type FROM user_costs
        UNION ALL
        SELECT name, cost_usd, type FROM role_costs
        ORDER BY cost_usd DESC
        LIMIT 10
    """,

    "percentage_high_cost_users": """
        WITH warehouse_rates AS (
            SELECT * FROM VALUES
                ('X-Small', 1), ('Small', 2), ('Medium', 4), ('Large', 8),
                ('X-Large', 16), ('2X-Large', 32), ('3X-Large', 64), ('4X-Large', 128)
            AS warehouse_size(size, credits_per_hour)
        ),
        user_total_costs AS (
            SELECT
                qh.user_name,
                SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0) AS total_user_cost_usd
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.warehouse_name IS NOT NULL
            AND qh.user_name IS NOT NULL
            {user_filter}
            GROUP BY qh.user_name
        ),
        cost_statistics AS (
            SELECT
                COALESCE(AVG(total_user_cost_usd), 0) AS overall_average_cost,
                COUNT(*) AS total_active_user_count
            FROM user_total_costs
        ),
        high_cost_users AS (
            SELECT
                urc.user_name,
                urc.total_user_cost_usd,
                cs.overall_average_cost
            FROM user_total_costs urc
            CROSS JOIN cost_statistics cs
            WHERE cs.overall_average_cost > 0
            AND urc.total_user_cost_usd >= 1.5 * cs.overall_average_cost
        )
        SELECT
            COALESCE(
                ROUND(
                    (SELECT COUNT(*) FROM high_cost_users) * 100.0 /
                    NULLIF((SELECT total_active_user_count FROM cost_statistics), 0),
                    2
                ),
                0
            ) AS metric_value
    """,

    "total_queries_run": """
        SELECT COUNT(*) AS metric_value
        FROM snowflake.account_usage.query_history
        WHERE start_time >= '{start_date}'
            AND user_name IS NOT NULL
            {user_filter}
            AND query_type NOT IN ('DESCRIBE', 'SHOW', 'USE')
            AND execution_status IN ('SUCCESS', 'FAIL');
    """,

    "total_users_defined": """
        SELECT
            COUNT(*) AS metric_value
        FROM snowflake.account_usage.users
        WHERE deleted_on IS NULL
    """,

    "total_active_users": """
        SELECT COUNT(DISTINCT user_name) AS metric_value
        FROM snowflake.account_usage.query_history
        WHERE start_time >= '{start_date}'
        AND user_name IS NOT NULL
        {user_filter}
    """,

    "avg_cost_per_user": """
        WITH warehouse_rates AS (
            SELECT * FROM VALUES
                ('X-Small', 1), ('Small', 2), ('Medium', 4), ('Large', 8),
                ('X-Large', 16), ('2X-Large', 32), ('3X-Large', 64), ('4X-Large', 128)
            AS warehouse_size(size, credits_per_hour)
        ),
        user_costs AS (
            SELECT
                qh.user_name,
                ROUND(SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0), 2) AS user_cost
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.user_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
            {user_filter}
            GROUP BY qh.user_name
        )
        SELECT COALESCE(ROUND(AVG(user_cost), 2), 0) AS metric_value
        FROM user_costs
    """,

    "high_cost_users_count": """
        WITH warehouse_rates AS (
            SELECT * FROM VALUES
                ('X-Small', 1), ('Small', 2), ('Medium', 4), ('Large', 8),
                ('X-Large', 16), ('2X-Large', 32), ('3X-Large', 64), ('4X-Large', 128)
            AS warehouse_size(size, credits_per_hour)
        ),
        user_costs AS (
            SELECT
                qh.user_name,
                SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0) AS user_cost
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.user_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
            {user_filter}
            GROUP BY qh.user_name
            HAVING SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0) > 100
        )
        SELECT COUNT(*) AS metric_value FROM user_costs
    """,

    "failed_queries_percentage": """
        WITH query_stats AS (
            SELECT
                COUNT(*) AS total_queries,
                COUNT(CASE WHEN execution_status = 'FAIL' THEN 1 END) AS failed_queries
            FROM snowflake.account_usage.query_history
            WHERE start_time >= '{start_date}'
            {user_filter}
        )
        SELECT COALESCE(ROUND((failed_queries * 100.0 / NULLIF(total_queries, 0)), 2), 0) AS metric_value
        FROM query_stats
    """,

    "avg_query_duration": """
        SELECT COALESCE(ROUND(AVG(total_elapsed_time) / 1000.0, 2), 0) AS metric_value
        FROM snowflake.account_usage.query_history
        WHERE start_time >= '{start_date}'
        AND total_elapsed_time > 0
        AND execution_status = 'SUCCESS'
        {user_filter}
    """,

    "cost_by_user_priority": """
        WITH warehouse_rates AS (
            SELECT * FROM VALUES
                ('X-Small', 1), ('Small', 2), ('Medium', 4), ('Large', 8),
                ('X-Large', 16), ('2X-Large', 32), ('3X-Large', 64), ('4X-Large', 128)
            AS warehouse_size(size, credits_per_hour)
        ),
        user_raw_costs AS (
            SELECT
                qh.user_name,
                SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0) AS raw_total_cost_usd,
                COUNT(DISTINCT qh.query_id) AS query_count,
                AVG(qh.total_elapsed_time / 1000.0) AS raw_avg_duration_sec,
                COUNT(CASE WHEN qh.execution_status = 'FAIL' THEN 1 END) AS failed_queries
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.user_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
            {user_filter}
            GROUP BY qh.user_name
        ),
        user_avg_cost AS (
            SELECT
                COALESCE(AVG(raw_total_cost_usd), 0) AS overall_avg_cost
            FROM user_raw_costs
        )
        SELECT
            urc.user_name AS USER_NAME,
            ROUND(urc.raw_total_cost_usd, 2) AS TOTAL_COST_USD,
            urc.query_count AS QUERY_COUNT,
            ROUND(urc.raw_avg_duration_sec, 2) AS AVG_DURATION_SEC,
            urc.failed_queries AS FAILED_QUERIES,
            CASE
               WHEN urc.raw_total_cost_usd >= 2.0 * uac.overall_avg_cost THEN 'Critical Cost Risk 🔴'
WHEN urc.raw_total_cost_usd >= 1.5 * uac.overall_avg_cost THEN 'High Cost Exposure 🟠'
WHEN urc.raw_total_cost_usd > uac.overall_avg_cost THEN 'Above Average Spend 🟡'
ELSE 'Optimized Usage 🟢'
            END AS PRIORITY_LEVEL
        FROM user_raw_costs urc
        CROSS JOIN user_avg_cost uac
        ORDER BY urc.raw_total_cost_usd DESC
        LIMIT 15
    """,

    "query_performance_bottlenecks": """
        WITH query_analysis AS (
            SELECT
                user_name,
                warehouse_name,
                query_type,
                total_elapsed_time / 1000.0 AS total_duration_sec,
                execution_status,
                partitions_scanned,
                bytes_scanned
            FROM snowflake.account_usage.query_history
            WHERE start_time >= '{start_date}'
            AND user_name IS NOT NULL
            {user_filter}
        )
        SELECT
            user_name AS USER_NAME,
            warehouse_name AS WAREHOUSE_NAME,
            query_type AS QUERY_TYPE,
            COUNT(*) AS QUERY_COUNT,
            ROUND(AVG(total_duration_sec), 2) AS AVG_DURATION_SEC,
            ROUND(MAX(total_duration_sec), 2) AS MAX_DURATION_SEC,
            COUNT(CASE WHEN total_duration_sec > 300 THEN 1 END) AS SLOW_QUERIES,
            COUNT(CASE WHEN execution_status = 'FAIL' THEN 1 END) AS FAILED_QUERIES,
            ROUND((COUNT(CASE WHEN total_duration_sec > 300 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)), 2) AS SLOW_QUERY_PERCENTAGE,
            CASE
                WHEN COUNT(CASE WHEN total_duration_sec > 300 THEN 1 END) > 0.1 * COUNT(*) THEN 'Critical'
                WHEN COUNT(CASE WHEN total_duration_sec > 60 THEN 1 END) > 0.1 * COUNT(*) THEN 'Warning'
                WHEN COUNT(CASE WHEN execution_status = 'FAIL' THEN 1 END) > 0.05 * COUNT(*) THEN 'Warning'
                ELSE 'Good'
            END AS PERFORMANCE_STATUS,
            CASE
                WHEN COUNT(CASE WHEN total_duration_sec > 300 THEN 1 END) > 0.1 * COUNT(*) THEN 'Optimize query logic, review data distribution, consider warehouse right-sizing or clustering.'
                WHEN COUNT(CASE WHEN execution_duration_sec = 'FAIL' THEN 1 END) > 0.05 * COUNT(*) THEN 'Investigate error logs, check permissions, validate SQL syntax.'
                WHEN AVG(total_duration_sec) > 60 AND AVG(total_duration_sec) <= 300 THEN 'Consider scaling warehouse, review query joins/filters, check caching.'
                ELSE 'Monitor performance regularly.'
            END AS RECOMMENDED_ACTION
        FROM query_analysis
        WHERE warehouse_name IS NOT NULL
        GROUP BY user_name, warehouse_name, query_type
        HAVING COUNT(*) > 5
        ORDER BY SLOW_QUERIES DESC, AVG_DURATION_SEC DESC
        LIMIT 20
    """,

    "user_behavior_patterns": """
        WITH warehouse_rates AS (
            SELECT * FROM VALUES
                ('X-Small', 1), ('Small', 2), ('Medium', 4), ('Large', 8),
                ('X-Large', 16), ('2X-Large', 32), ('3X-Large', 64), ('4X-Large', 128)
            AS warehouse_size(size, credits_per_hour)
        ),
        cost_ranked_users AS (
            SELECT
                qh.user_name,
                ROUND(SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0), 2) AS total_cost_usd
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.user_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
            {user_filter}
            GROUP BY qh.user_name
            ORDER BY total_cost_usd DESC
            LIMIT 10
        ),
        hourly_usage AS (
            SELECT
                qh.user_name,
                EXTRACT(HOUR FROM qh.start_time) AS hour_of_day,
                COUNT(*) AS query_count,
                ROUND(AVG(qh.total_elapsed_time / 1000.0), 2) AS avg_duration
            FROM snowflake.account_usage.query_history qh
            WHERE qh.start_time >= '{start_date}'
            AND qh.user_name IN (SELECT user_name FROM cost_ranked_users)
            {user_filter}
            GROUP BY qh.user_name, EXTRACT(HOUR FROM qh.start_time)
        )
        SELECT
            user_name AS USER_NAME,
            hour_of_day AS HOUR_OF_DAY,
            SUM(query_count) AS TOTAL_QUERIES,
            ROUND(AVG(query_count), 2) AS AVG_QUERIES_PER_HOUR,
            ROUND(AVG(avg_duration), 2) AS AVG_DURATION_SEC,
            CASE
                WHEN hour_of_day BETWEEN 0 AND 6 THEN 'Off-Hours'
                WHEN hour_of_day BETWEEN 7 AND 18 THEN 'Business Hours'
                ELSE 'Evening'
            END AS TIME_CATEGORY
        FROM hourly_usage
        GROUP BY user_name, hour_of_day
        ORDER BY user_name, hour_of_day
    """,

    "optimization_opportunities": """
        WITH warehouse_rates AS (
            SELECT * FROM VALUES
                ('X-Small', 1), ('Small', 2), ('Medium', 4), ('Large', 8),
                ('X-Large', 16), ('2X-Large', 32), ('3X-Large', 64), ('4X-Large', 128)
            AS warehouse_size(size, credits_per_hour)
        ),
        cost_ranked_users AS (
            SELECT
                qh.user_name,
                SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0) AS total_cost_usd
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.user_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
            {user_filter}
            GROUP BY qh.user_name
            ORDER BY total_cost_usd DESC
            LIMIT 10
        ),
        optimization_analysis AS (
            SELECT
                qh.user_name,
                qh.warehouse_name,
                COUNT(qh.query_id) AS total_queries,
                COUNT(CASE WHEN qh.total_elapsed_time > 300000 THEN 1 END) AS long_queries,
                COUNT(CASE WHEN qh.execution_status = 'FAIL' THEN 1 END) AS failed_queries,
                COUNT(CASE WHEN COALESCE(qh.bytes_scanned, 0) > 1000000000 THEN 1 END) AS high_scan_queries,
                SUM(qh.total_elapsed_time / 1000.0 / 3600.0 * wr.credits_per_hour * 3.0) AS total_cost_usd,
                AVG(qh.total_elapsed_time / 1000.0) AS avg_duration
            FROM snowflake.account_usage.query_history qh
            JOIN warehouse_rates wr ON qh.warehouse_size = wr.size
            WHERE qh.start_time >= '{start_date}'
            AND qh.user_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
            AND qh.user_name IN (SELECT user_name FROM cost_ranked_users)
            {user_filter}
            GROUP BY qh.user_name, qh.warehouse_name
        )
        SELECT
            user_name AS USER_NAME,
            warehouse_name AS WAREHOUSE_NAME,
            total_queries AS TOTAL_QUERIES,
            long_queries AS LONG_QUERIES,
            failed_queries AS FAILED_QUERIES,
            high_scan_queries AS HIGH_SCAN_QUERIES,
            ROUND(total_cost_usd, 2) AS TOTAL_COST_USD,
            ROUND(avg_duration, 2) AS AVG_DURATION_SEC,
            ROUND((long_queries * 100.0 / NULLIF(total_queries, 0)), 2) AS LONG_QUERY_PERCENTAGE,
            ROUND((failed_queries * 100.0 / NULLIF(total_queries, 0)), 2) AS FAILURE_RATE,
            CASE
                WHEN total_cost_usd > 1000 AND long_queries > 0.1 * total_queries THEN 'High Cost & Slow Queries'
                WHEN total_cost_usd > 500 THEN 'High Cost User/Warehouse'
                WHEN long_queries > 0.2 * total_queries THEN 'Frequent Long Queries'
                WHEN failed_queries > 0.1 * total_queries THEN 'High Query Failure Rate'
                WHEN high_scan_queries > 0.3 * total_queries THEN 'High Data Scan (Inefficient Queries)'
                ELSE 'Good Performance'
            END AS OPTIMIZATION_PRIORITY,
            CASE
                WHEN total_cost_usd > 1000 AND long_queries > 0.1 * total_queries THEN 'Review expensive queries, right-size warehouse, implement auto-suspend, consider clustering.'
                WHEN total_cost_usd > 500 THEN 'Analyze user query patterns, optimize frequently run queries, right-size warehouse.'
                WHEN long_queries > 0.2 * total_queries THEN 'Tune query filters, joins, use appropriate data types, consider materialized views.'
                WHEN failed_queries > 0.1 * total_queries THEN 'Debug query errors, check user permissions, validate data integrity.'
                WHEN high_scan_queries > 0.3 * total_queries THEN 'Implement table clustering, optimize micro-partitioning, review query WHERE clauses.'
                ELSE 'Continue monitoring for efficiency gains.'
            END AS RECOMMENDED_ACTION
        FROM optimization_analysis
        WHERE total_queries > 10
        ORDER BY TOTAL_COST_USD DESC, LONG_QUERY_PERCENTAGE DESC
    """
}
