# finops_dashboard/src/metric_builder.py

import streamlit as st
from typing import Optional, Literal, Any
import logging

# Import necessary components from other modules
from src.config import METRIC_FORMATS # For formatting rules
from src.data_processor import DataProcessor # For delta calculations and color determination
from src.utils import handle_errors # For robust operations

logger = logging.getLogger(__name__)

class MetricBuilder:
    """
    Builds and displays standardized Streamlit metric cards (st.metric).
    It handles value formatting, delta calculations, and delta color styling.
    """

    @staticmethod
    def _format_value(value: Optional[Any], metric_type: str) -> str:
        """
        Formats a numeric value based on predefined metric types from config.
        """
        if value is None:
            return "N/A"

        format_spec = METRIC_FORMATS.get(metric_type, METRIC_FORMATS["float_number"]) # Default to float_number
        
        prefix = format_spec.get("prefix", "")
        suffix = format_spec.get("suffix", "")
        decimals = format_spec.get("decimals", 2)
        
        # Format the number
        try:
            formatted_value = f"{value:.{decimals}f}"
        except (TypeError, ValueError):
            logger.warning(f"Could not format value '{value}' as a number for type '{metric_type}'. Displaying as is.")
            formatted_value = str(value)
        
        return f"{prefix}{formatted_value}{suffix}"

    @staticmethod
    @handle_errors
    def build_metric_card(
        label: str,
        current_value: Optional[Any],
        previous_value: Optional[Any] = None,
        metric_type: Literal["number", "float_number", "percentage", "currency", "duration_seconds"] = "float_number",
        higher_is_better_for_delta: bool = True,
        value_suffix: str = "" # Optional suffix to append to the formatted value for display (overrides config suffix)
    ) -> None:
        """
        Builds and displays a Streamlit metric card with optional delta.

        Args:
            label (str): The label displayed above the metric value.
            current_value (Optional[Any]): The main value to display.
            previous_value (Optional[Any]): The value from a previous period for delta calculation.
                                            If None, no delta is displayed.
            metric_type (Literal[...]): Defines how the value should be formatted
                                         (e.g., "currency", "percentage"). Must match keys in config.METRIC_FORMATS.
            higher_is_better_for_delta (bool): True if a higher value is a positive change for delta coloring.
            value_suffix (str): An additional suffix to display after the formatted value.
                                Overrides the suffix from METRIC_FORMATS if provided.
        """
        formatted_current_value = MetricBuilder._format_value(current_value, metric_type)
        
        delta_value: Optional[float] = None
        delta_string: Optional[str] = None
        delta_color: Literal["normal", "inverse", "off"] = "off"
        
        if previous_value is not None:
            delta_value = DataProcessor.calculate_percentage_delta(current_value, previous_value)
            if delta_value is not None:
                delta_string = f"{delta_value:.1f}%"
                delta_color = DataProcessor.determine_delta_color(delta_value, higher_is_better_for_delta)
            else:
                delta_string = "N/A"
                delta_color = "off"
        
        # Apply custom CSS class for delta color control if needed
        # Streamlit's st.metric already handles 'normal', 'inverse', 'off'
        # but we add a custom class for potential direct CSS overrides if desired.
        css_class = f"delta-color-{delta_color}" if delta_color else ""
        
        # Render the st.metric component.
        # Streamlit itself handles the delta arrow and basic coloring based on `delta_color`.
        st.metric(
            label=label,
            value=f"{formatted_current_value}{value_suffix}",
            delta=delta_string,
            delta_color=delta_color,
            help=f"Current: {formatted_current_value}. Previous: {MetricBuilder._format_value(previous_value, metric_type) if previous_value is not None else 'N/A'}"
        )