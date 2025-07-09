# finops_dashboard/src/metric_builder.py

import streamlit as st
import logging
from typing import Optional, Union

# Import utilities and configuration
from src.utils import handle_errors
from src.config import METRIC_FORMATS # For consistent number formatting
from src.data_processor import DataProcessor # For delta calculation and color

logger = logging.getLogger(__name__)

class MetricBuilder:
    """
    Responsible for rendering Streamlit metric cards with consistent formatting
    and delta calculations.
    """

    @staticmethod
    @handle_errors
    def build_metric_card(
        label: str,
        current_value: Optional[Union[int, float]],
        previous_value: Optional[Union[int, float]] = None,
        metric_type: str = "number", # Corresponds to keys in METRIC_FORMATS in config.py
        higher_is_better_for_delta: bool = True, # For delta color logic
        value_prefix: str = "", # Additional prefix specific to this metric
        value_suffix: str = ""  # Additional suffix specific to this metric
    ):
        """
        Builds and displays a Streamlit metric card with optional delta.

        Args:
            label (str): The label for the metric (e.g., "Total Credits Used").
            current_value (Optional[Union[int, float]]): The current value of the metric.
            previous_value (Optional[Union[int, float]], optional): The value from a previous period
                                                                      for delta calculation. Defaults to None.
            metric_type (str): Key from METRIC_FORMATS in config.py to apply specific formatting
                                (e.g., "currency", "percentage", "number"). Defaults to "number".
            higher_is_better_for_delta (bool): If True, a positive delta is green.
                                                If False, a negative delta is green (e.g., for cost).
                                                Defaults to True.
            value_prefix (str): An additional string prefix to prepend to the formatted value.
            value_suffix (str): An additional string suffix to append to the formatted value.
        """
        # Get formatting rules from config
        format_config = METRIC_FORMATS.get(metric_type, METRIC_FORMATS["number"])
        
        prefix = value_prefix + format_config.get("prefix", "")
        suffix = format_config.get("suffix", "") + value_suffix
        decimals = format_config.get("decimals", 0)
        thousands_sep = format_config.get("thousands_sep", False)

        # Format current value
        if current_value is None:
            formatted_value = "N/A"
            logger.debug(f"Metric '{label}': Current value is None.")
        else:
            if thousands_sep:
                formatted_value = f"{current_value:,.{decimals}f}" # Format with comma separator
            else:
                formatted_value = f"{current_value:.{decimals}f}" # Format without comma separator
            formatted_value = f"{prefix}{formatted_value}{suffix}"

        # Calculate and format delta if previous_value is provided
        delta_str: Optional[str] = None
        delta_color: str = "off"

        if previous_value is not None:
            # Use DataProcessor for consistent delta calculation
            delta_str = DataProcessor.calculate_percentage_delta(current_value, previous_value)
            
            # Use DataProcessor for consistent delta color determination
            # Note: For costs, higher_is_better_for_delta would be False (lower cost is better)
            delta_color = DataProcessor.determine_delta_color(
                current_value, previous_value, higher_is_better=higher_is_better_for_delta
            )
            
            # If current_value is None or previous_value is None, delta_str would be None
            # In that case, we want delta_color to be 'off' and delta_str to indicate it.
            if delta_str is None:
                delta_color = "off"
                delta_str = "N/A"
        
        st.metric(label=label, value=formatted_value, delta=delta_str, delta_color=delta_color)
        logger.debug(f"Metric '{label}' displayed with value '{formatted_value}' and delta '{delta_str}'.")