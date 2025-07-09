# finops_dashboard/src/data_processor.py

import pandas as pd
from typing import Optional, Tuple, Literal, Dict, Any, List
import logging

# Import constants from config.py for priority levels
from src.config import PRIORITY_LEVELS
from src.utils import handle_errors # For robust operations

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Handles all data processing, transformation, and business logic calculations.
    This class operates on Pandas DataFrames, typically after they have been fetched
    from Snowflake by the DataFetcher.
    """

    @staticmethod
    def _safe_float(value: Any) -> float:
        """
        Safely converts a value to a float, handling None or non-numeric types gracefully.
        Returns 0.0 if conversion fails.
        """
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert value '{value}' to float. Defaulting to 0.0.")
            return 0.0

    @staticmethod
    @handle_errors
    def calculate_percentage_delta(current_value: Optional[float], previous_value: Optional[float]) -> Optional[float]:
        """
        Calculates the percentage change between a current and a previous value.

        Args:
            current_value (Optional[float]): The current period's value.
            previous_value (Optional[float]): The previous period's value.

        Returns:
            Optional[float]: The percentage delta (e.g., 10.5 for 10.5% increase), or None if calculation not possible.
        """
        current_val_f = DataProcessor._safe_float(current_value)
        previous_val_f = DataProcessor._safe_float(previous_value)

        if previous_val_f == 0 and current_val_f == 0:
            return 0.0 # No change if both are zero
        if previous_val_f == 0:
            # Handle cases where previous was zero but current is not.
            # This is an "infinite" increase, which we can represent as 100% for display purposes,
            # or a very large number, or just indicate no previous data.
            # For simplicity, if previous is zero and current is positive, it's a 100% increase from zero base.
            # If previous is zero and current is negative, it's a 100% decrease.
            return 100.0 if current_val_f > 0 else -100.0
        
        return ((current_val_f - previous_val_f) / previous_val_f) * 100

    @staticmethod
    def determine_delta_color(delta: Optional[float], higher_is_better: bool) -> Literal["normal", "inverse", "off"]:
        """
        Determines the color for a Streamlit st.metric delta based on the value
        and whether a higher value is considered better.

        Args:
            delta (Optional[float]): The percentage delta value.
            higher_is_better (bool): True if a higher value is considered a positive change.

        Returns:
            Literal["normal", "inverse", "off"]: "normal" (green/positive), "inverse" (red/negative), or "off" (gray/no change).
        """
        if delta is None:
            return "off" # Grey if delta cannot be calculated

        if delta > 0:
            return "normal" if higher_is_better else "inverse"
        elif delta < 0:
            return "inverse" if higher_is_better else "normal"
        else:
            return "off" # No change

    @staticmethod
    @handle_errors
    def identify_high_impact_users(
        users_df: pd.DataFrame,
        cost_column: str,
        user_column: str,
        high_cost_threshold_percentile: float = 90, # Users above this percentile are 'High'
        medium_cost_threshold_percentile: float = 70 # Users above this percentile are 'Medium'
    ) -> pd.DataFrame:
        """
        Identifies high, medium, and low impact users based on their cost (credits used).
        Adds 'PRIORITY_LEVEL', 'PRIORITY_LABEL', and 'PRIORITY_ICON' columns to the DataFrame.

        Args:
            users_df (pd.DataFrame): DataFrame containing user names and their costs.
                                     Expected columns: user_column, cost_column.
            cost_column (str): The name of the column containing cost/credit values.
            user_column (str): The name of the column containing user names.
            high_cost_threshold_percentile (float): Percentile above which a user is 'High Priority'.
            medium_cost_threshold_percentile (float): Percentile above which a user is 'Medium Priority'.

        Returns:
            pd.DataFrame: The DataFrame with added 'PRIORITY_LEVEL', 'PRIORITY_LABEL', and 'PRIORITY_ICON' columns.
        """
        if users_df.empty or cost_column not in users_df.columns or user_column not in users_df.columns:
            logger.warning("Input DataFrame for identify_high_impact_users is empty or missing required columns.")
            # Return an empty DataFrame with expected columns if input is invalid
            return pd.DataFrame(columns=[user_column, cost_column, 'PRIORITY_LEVEL', 'PRIORITY_LABEL', 'PRIORITY_ICON'])

        df = users_df.copy() # Work on a copy to avoid modifying original DataFrame
        
        # Calculate percentile thresholds
        high_threshold = df[cost_column].quantile(high_cost_threshold_percentile / 100)
        medium_threshold = df[cost_column].quantile(medium_cost_threshold_percentile / 100)

        def assign_priority(row: pd.Series) -> Dict[str, str]:
            cost = row[cost_column]
            if cost >= high_threshold:
                return PRIORITY_LEVELS["High Priority"]
            elif cost >= medium_threshold:
                return PRIORITY_LEVELS["Medium Priority"]
            else:
                return PRIORITY_LEVELS["Low Priority"]

        # Apply the priority assignment
        priority_info = df.apply(assign_priority, axis=1)
        df['PRIORITY_LABEL'] = [p['label'] for p in priority_info]
        df['PRIORITY_ICON'] = [p['icon'] for p in priority_info]
        # Store a simple level for sorting or further processing if needed
        df['PRIORITY_LEVEL'] = df['PRIORITY_LABEL'].map({
            PRIORITY_LEVELS["High Priority"]["label"]: 3,
            PRIORITY_LEVELS["Medium Priority"]["label"]: 2,
            PRIORITY_LEVELS["Low Priority"]["label"]: 1
        })
        
        return df

    @staticmethod
    @handle_errors
    def get_top_n_values(
        df: pd.DataFrame,
        value_col: str,
        name_col: str,
        n: int = 5,
        other_label: str = "Others"
    ) -> pd.DataFrame:
        """
        Gets the top N values from a DataFrame and aggregates the rest into an 'Others' category.

        Args:
            df (pd.DataFrame): Input DataFrame with a value column and a name column.
            value_col (str): The name of the column containing values to sort by (e.g., 'TOTAL_CREDITS_USED').
            name_col (str): The name of the column containing names (e.g., 'USER_NAME').
            n (int): The number of top values to retrieve.
            other_label (str): The label for the aggregated 'Others' category.

        Returns:
            pd.DataFrame: A new DataFrame with top N entries and an 'Others' entry,
                          sorted by value in descending order.
        """
        if df.empty or value_col not in df.columns or name_col not in df.columns:
            logger.warning(f"Input DataFrame for get_top_n_values is empty or missing required columns ({value_col}, {name_col}).")
            return pd.DataFrame(columns=[name_col, value_col])

        # Ensure value column is numeric and handle potential NaNs
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)

        # Sort and get top N
        sorted_df = df.sort_values(by=value_col, ascending=False).reset_index(drop=True)
        top_n_df = sorted_df.head(n)

        # Aggregate the rest into 'Others'
        if len(sorted_df) > n:
            others_sum = sorted_df.iloc[n:][value_col].sum()
            others_row = pd.DataFrame([{name_col: other_label, value_col: others_sum}])
            result_df = pd.concat([top_n_df, others_row], ignore_index=True)
        else:
            result_df = top_n_df

        return result_df.sort_values(by=value_col, ascending=False).reset_index(drop=True)

    @staticmethod
    @handle_errors
    def pivot_for_heatmap(
        df: pd.DataFrame,
        index_col: str,
        column_col: str,
        value_col: str,
        agg_func: str = 'sum'
    ) -> Optional[pd.DataFrame]:
        """
        Pivots a DataFrame for use in a heatmap.

        Args:
            df (pd.DataFrame): The input DataFrame.
            index_col (str): The column to use as the new index (rows).
            column_col (str): The column to use as the new columns.
            value_col (str): The column whose values will fill the pivot table.
            agg_func (str): The aggregation function ('sum', 'mean', 'count', etc.).

        Returns:
            Optional[pd.DataFrame]: A pivoted DataFrame, or None if input is invalid or empty.
        """
        if df.empty or not all(col in df.columns for col in [index_col, column_col, value_col]):
            logger.warning(f"Input DataFrame for pivot_for_heatmap is empty or missing required columns: {index_col}, {column_col}, {value_col}.")
            return None
        
        # Ensure value column is numeric before pivoting
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)

        try:
            pivoted_df = df.pivot_table(
                index=index_col,
                columns=column_col,
                values=value_col,
                aggfunc=agg_func
            )
            # Fill NaN values (e.g., where a combination of index/column doesn't exist)
            pivoted_df = pivoted_df.fillna(0)
            return pivoted_df
        except Exception as e:
            logger.error(f"Error pivoting DataFrame: {e}", exc_info=True)
            return None