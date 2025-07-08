# finops_dashboard/src/data_processor.py

import pandas as pd
from typing import Optional, Union, Dict, Any
import logging

# Import utilities for error handling
from src.utils import handle_errors
# Import config for priority levels or other thresholds
from src.config import PRIORITY_LEVELS

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Handles post-query data manipulation, transformations, and business logic.
    This class does NOT fetch data; it processes DataFrames it receives.
    """

    @staticmethod
    @handle_errors
    def calculate_percentage_delta(current_value: Optional[Union[int, float]], previous_value: Optional[Union[int, float]]) -> Optional[str]:
        """
        Calculates the percentage change between two values.
        Returns a formatted string for delta display.
        Assumes higher current_value is generally 'better' for delta color determination.
        """
        if current_value is None or previous_value is None:
            return None # Cannot calculate delta if either value is missing
        
        if previous_value == 0:
            if current_value == 0:
                return "0.0%" # No change from zero
            else:
                return "N/A" # Cannot divide by zero for percentage change
        
        delta = ((current_value - previous_value) / previous_value) * 100
        
        # Determine sign for delta formatting
        sign = "+" if delta >= 0 else ""
        return f"{sign}{delta:.1f}%"

    @staticmethod
    @handle_errors
    def determine_delta_color(current_value: Optional[Union[int, float]], previous_value: Optional[Union[int, float]], higher_is_better: bool = False) -> str:
        """
        Determines the appropriate delta color ('normal', 'inverse', 'off') for Streamlit metrics.
        
        Args:
            current_value: The current metric value.
            previous_value: The previous metric value.
            higher_is_better: If True, higher values are good (green delta). If False, lower values are good (green delta).
        
        Returns:
            str: 'normal' (green for positive, red for negative), 'inverse' (red for positive, green for negative), 'off'.
        """
        if current_value is None or previous_value is None or current_value == previous_value:
            return "off" # No delta or no change

        if current_value > previous_value:
            return "normal" if higher_is_better else "inverse" # 'normal' means green for positive change
        else: # current_value < previous_value
            return "normal" if not higher_is_better else "inverse" # 'inverse' means red for positive change


    @staticmethod
    @handle_errors
    def pivot_for_heatmap(
        df: pd.DataFrame, 
        index_col: str, 
        columns_col: str, 
        values_col: str,
        fill_value: Union[int, float] = 0
    ) -> pd.DataFrame:
        """
        Pivots a DataFrame for heatmap rendering.
        Handles potential errors during pivoting and fills NaN values.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            index_col (str): Column to use for the new DataFrame's index.
            columns_col (str): Column to use for the new DataFrame's columns.
            values_col (str): Column to use for the values in the pivoted DataFrame.
            fill_value (Union[int, float]): Value to replace NaN entries with after pivoting.
            
        Returns:
            pd.DataFrame: The pivoted DataFrame. Returns empty DataFrame on error or if input is empty.
        """
        if df.empty:
            logger.warning("Input DataFrame for pivot_for_heatmap is empty.")
            return pd.DataFrame()
            
        required_cols = [index_col, columns_col, values_col]
        if not all(col in df.columns for col in required_cols):
            logger.error(f"Missing required columns for pivot: {', '.join([col for col in required_cols if col not in df.columns])}")
            st.error(f"Error: Missing columns for heatmap data ({index_col}, {columns_col}, {values_col}).")
            return pd.DataFrame()

        try:
            # Ensure the values column is numeric
            df[values_col] = pd.to_numeric(df[values_col], errors='coerce').fillna(fill_value)
            
            pivot_df = df.pivot_table(
                index=index_col, 
                columns=columns_col, 
                values=values_col,
                fill_value=fill_value # Fill NaN during pivot
            )
            # Ensure column order if needed (e.g., for hours 0-23)
            if columns_col == 'QUERY_HOUR' and all(col in pivot_df.columns for col in range(24)):
                pivot_df = pivot_df[[i for i in range(24) if i in pivot_df.columns]]
            
            return pivot_df
        except KeyError as e:
            logger.error(f"KeyError during pivot for heatmap: {e}. Check column names and data.", exc_info=True)
            st.error(f"Error pivoting data for heatmap. Check column names.")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error pivoting DataFrame for heatmap: {e}", exc_info=True)
            st.error(f"An unexpected error occurred during data pivoting: {e}")
            return pd.DataFrame()

    @staticmethod
    @handle_errors
    def identify_high_impact_users(
        df: pd.DataFrame, 
        cost_column: str = "TOTAL_CREDITS_USED", 
        user_column: str = "USER_NAME",
        avg_cost_threshold_multiplier: float = 1.5 # e.g., 1.5x average cost for "Medium Priority"
    ) -> pd.DataFrame:
        """
        Identifies users with costs significantly above the average and assigns priority levels.
        Assumes df contains user_column and cost_column.
        
        Args:
            df (pd.DataFrame): DataFrame with user cost data.
            cost_column (str): Name of the column containing cost values (e.g., 'TOTAL_CREDITS_USED').
            user_column (str): Name of the column containing user names.
            avg_cost_threshold_multiplier (float): Multiplier for average cost to determine 'Medium Priority'.

        Returns:
            pd.DataFrame: Original DataFrame with added 'PRIORITY_LEVEL', 'PRIORITY_LABEL',
                          'PRIORITY_BG_COLOR', 'PRIORITY_TEXT_COLOR' columns.
        """
        if df.empty or cost_column not in df.columns or user_column not in df.columns:
            logger.warning(f"Input DataFrame empty or missing required columns for high impact user identification: {user_column}, {cost_column}.")
            return df # Return original if missing data/columns

        # Ensure cost column is numeric before calculating mean
        df[cost_column] = pd.to_numeric(df[cost_column], errors='coerce').fillna(0)
        
        avg_cost = df[cost_column].mean()
        
        # Assign priority levels based on thresholds defined here
        # These map directly to keys in PRIORITY_LEVELS from config.py
        def assign_priority(row_cost):
            if avg_cost == 0: # Avoid division by zero, or if all costs are zero
                return "Good Performance"
            if row_cost > avg_cost * 2:
                return "High Priority"
            elif row_cost > avg_cost * avg_cost_threshold_multiplier:
                return "Medium Priority"
            elif row_cost > avg_cost:
                return "Above Avg Cost"
            else:
                return "Good Performance"

        df['PRIORITY_LEVEL'] = df[cost_column].apply(assign_priority)
        
        # Add details from config for display in UIComponents
        df['PRIORITY_LABEL'] = df['PRIORITY_LEVEL'].apply(lambda x: PRIORITY_LEVELS.get(x, PRIORITY_LEVELS["N/A"]).get('label', x))
        df['PRIORITY_BG_COLOR'] = df['PRIORITY_LEVEL'].apply(lambda x: PRIORITY_LEVELS.get(x, PRIORITY_LEVELS["N/A"]).get('bg_color', ''))
        df['PRIORITY_TEXT_COLOR'] = df['PRIORITY_LEVEL'].apply(lambda x: PRIORITY_LEVELS.get(x, PRIORITY_LEVELS["N/A"]).get('text_color', ''))
        df['PRIORITY_ICON'] = df['PRIORITY_LEVEL'].apply(lambda x: PRIORITY_LEVELS.get(x, PRIORITY_LEVELS["N/A"]).get('icon', ''))


        return df

    @staticmethod
    @handle_errors
    def get_top_n_values(df: pd.DataFrame, value_col: str, name_col: str, n: int = 5) -> pd.DataFrame:
        """
        Returns the top N rows based on a value column, including an 'Others' category.
        
        Args:
            df (pd.DataFrame): Input DataFrame.
            value_col (str): Column to sort and sum by.
            name_col (str): Column for names (e.g., 'USER_NAME').
            n (int): Number of top items to include before 'Others'.
            
        Returns:
            pd.DataFrame: DataFrame with top N items and an 'Others' row. Returns empty DataFrame on error.
        """
        if df.empty or value_col not in df.columns or name_col not in df.columns:
            logger.warning(f"Input DataFrame empty or missing required columns for top N values: {value_col}, {name_col}.")
            return pd.DataFrame()

        # Ensure value column is numeric
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)

        df_sorted = df.sort_values(by=value_col, ascending=False).reset_index(drop=True)
        
        if len(df_sorted) <= n:
            return df_sorted
        
        top_n = df_sorted.head(n)
        others_sum = df_sorted.iloc[n:][value_col].sum()
        
        # Create a DataFrame for 'Others'
        # Ensure the column names match
        others_data = {name_col: 'Others', value_col: others_sum}
        # Add other columns from the original DataFrame if they are relevant and can be aggregated or defaulted
        # For simplicity, we only include the name_col and value_col.
        
        others_df = pd.DataFrame([others_data])
        
        return pd.concat([top_n, others_df], ignore_index=True)