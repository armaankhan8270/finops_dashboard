# finops_dashboard/src/chart_builder.py

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import logging
from typing import Optional, Dict, Any, List

# Import configuration for consistent styling
from src.config import PLOTLY_LAYOUT_DEFAULTS, ACCENT_COLOR_SCHEME, PRIORITY_LEVELS
from src.utils import handle_errors # For robust operations

logger = logging.getLogger(__name__)

class ChartBuilder:
    """
    Builds various interactive Plotly charts with consistent styling
    based on the application's configuration.
    """

    @staticmethod
    def _apply_default_layout(fig: go.Figure, title: str, x_axis_title: str, y_axis_title: str, **kwargs: Any) -> None:
        """
        Applies standard layout settings to a Plotly figure.

        Args:
            fig (go.Figure): The Plotly figure object to modify.
            title (str): The main title of the chart.
            x_axis_title (str): Label for the X-axis.
            y_axis_title (str): Label for the Y-axis.
            **kwargs: Additional layout parameters to override or add to defaults.
        """
        fig.update_layout(
            title_text=f"<b>{title}</b>",
            xaxis_title=x_axis_title,
            yaxis_title=y_axis_title,
            **PLOTLY_LAYOUT_DEFAULTS, # Apply global defaults
            **kwargs # Allow specific overrides
        )

    @staticmethod
    @handle_errors
    def build_line_chart(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        x_axis_title: str,
        y_axis_title: str,
        color_col: Optional[str] = None,
        hover_data: Optional[List[str]] = None
    ) -> Optional[go.Figure]:
        """
        Builds an interactive line chart.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            x_col (str): Column for the X-axis (e.g., date).
            y_col (str): Column for the Y-axis (e.g., credits).
            title (str): Title of the chart.
            x_axis_title (str): Label for the X-axis.
            y_axis_title (str): Label for the Y-axis.
            color_col (Optional[str]): Column to use for differentiating lines by color.
            hover_data (Optional[List[str]]): List of columns to show in hover tooltip.

        Returns:
            Optional[go.Figure]: A Plotly Figure object, or None if input data is invalid.
        """
        if df.empty or x_col not in df.columns or y_col not in df.columns:
            logger.warning(f"Input DataFrame for line chart is empty or missing required columns ({x_col}, {y_col}).")
            return None

        # Ensure x_col is datetime if it's a date/time column
        if pd.api.types.is_datetime64_any_dtype(df[x_col]):
            df[x_col] = pd.to_datetime(df[x_col])

        fig = px.line(
            df,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            color_discrete_sequence=ACCENT_COLOR_SCHEME,
            hover_data=hover_data
        )
        ChartBuilder._apply_default_layout(fig, title, x_axis_title, y_axis_title)
        return fig

    @staticmethod
    @handle_errors
    def build_bar_chart(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        x_axis_title: str,
        y_axis_title: str,
        orientation: Literal['v', 'h'] = 'v', # 'v' for vertical, 'h' for horizontal
        color_col: Optional[str] = None,
        hover_data: Optional[List[str]] = None
    ) -> Optional[go.Figure]:
        """
        Builds an interactive bar chart.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            x_col (str): Column for the X-axis.
            y_col (str): Column for the Y-axis.
            title (str): Title of the chart.
            x_axis_title (str): Label for the X-axis.
            y_axis_title (str): Label for the Y-axis.
            orientation (Literal['v', 'h']): Orientation of the bars ('v' for vertical, 'h' for horizontal).
            color_col (Optional[str]): Column to use for differentiating bars by color.
            hover_data (Optional[List[str]]): List of columns to show in hover tooltip.

        Returns:
            Optional[go.Figure]: A Plotly Figure object, or None if input data is invalid.
        """
        if df.empty or x_col not in df.columns or y_col not in df.columns:
            logger.warning(f"Input DataFrame for bar chart is empty or missing required columns ({x_col}, {y_col}).")
            return None

        color_map: Optional[Dict[str, str]] = None
        if color_col and color_col in df.columns and any(p_level in df[color_col].unique() for p_level in PRIORITY_LEVELS.keys()):
            # Create a color map based on defined priority levels
            color_map = {
                PRIORITY_LEVELS["High Priority"]["label"]: PRIORITY_LEVELS["High Priority"]["text_color"],
                PRIORITY_LEVELS["Medium Priority"]["label"]: PRIORITY_LEVELS["Medium Priority"]["text_color"],
                PRIORITY_LEVELS["Low Priority"]["label"]: PRIORITY_LEVELS["Low Priority"]["text_color"],
                PRIORITY_LEVELS["Normal"]["label"]: PRIORITY_LEVELS["Normal"]["text_color"],
                "Others": "#616161", # Default color for 'Others' if not explicitly a priority level
                "N/A": PRIORITY_LEVELS["N/A"]["text_color"]
            }
            # Filter color_map to only include colors for present values to avoid Plotly warnings
            color_map = {k: v for k, v in color_map.items() if k in df[color_col].unique()}
        else:
            # Fallback to general accent color scheme if no specific color mapping is needed
            color_discrete_sequence = ACCENT_COLOR_SCHEME


        if orientation == 'h':
            fig = px.bar(
                df,
                x=y_col,
                y=x_col,
                orientation='h',
                color=color_col,
                color_discrete_sequence=color_discrete_sequence if not color_map else None,
                color_discrete_map=color_map if color_map else None,
                title=title,
                hover_data=hover_data
            )
            fig.update_yaxes(categoryorder='total ascending') # Useful for horizontal bars
        else:
            fig = px.bar(
                df,
                x=x_col,
                y=y_col,
                orientation='v',
                color=color_col,
                color_discrete_sequence=color_discrete_sequence if not color_map else None,
                color_discrete_map=color_map if color_map else None,
                title=title,
                hover_data=hover_data
            )
            fig.update_xaxes(categoryorder='total descending') # Useful for vertical bars

        ChartBuilder._apply_default_layout(fig, title, x_axis_title, y_axis_title)
        return fig

    @staticmethod
    @handle_errors
    def build_pie_chart(
        df: pd.DataFrame,
        names_col: str,
        values_col: str,
        title: str,
        hole: float = 0.3, # For a donut chart
        hover_data: Optional[List[str]] = None
    ) -> Optional[go.Figure]:
        """
        Builds an interactive pie or donut chart.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            names_col (str): Column for slice names (e.g., 'STATUS').
            values_col (str): Column for slice values (e.g., 'COUNT').
            title (str): Title of the chart.
            hole (float): Value between 0 and 1 for donut chart (0 for pie chart).
            hover_data (Optional[List[str]]): List of columns to show in hover tooltip.

        Returns:
            Optional[go.Figure]: A Plotly Figure object, or None if input data is invalid.
        """
        if df.empty or names_col not in df.columns or values_col not in df.columns:
            logger.warning(f"Input DataFrame for pie chart is empty or missing required columns ({names_col}, {values_col}).")
            return None

        fig = px.pie(
            df,
            names=names_col,
            values=values_col,
            title=title,
            hole=hole,
            color_discrete_sequence=ACCENT_COLOR_SCHEME,
            hover_data=hover_data
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        ChartBuilder._apply_default_layout(fig, title, "", "") # No axis titles for pie charts
        return fig

    @staticmethod
    @handle_errors
    def build_heatmap(
        df: pd.DataFrame,
        x_labels: List[Any],
        y_labels: List[Any],
        z_data: pd.DataFrame, # Should be the pivoted DataFrame directly
        title: str,
        x_axis_title: str,
        y_axis_title: str,
        colorscale: str = 'Blues', # Example colorscale
        zmin: Optional[float] = None,
        zmax: Optional[float] = None
    ) -> Optional[go.Figure]:
        """
        Builds an interactive heatmap.

        Args:
            df (pd.DataFrame): The original DataFrame (not pivoted, but used for type hint consistency).
                               The actual data for the heatmap comes from z_data.
            x_labels (List[Any]): Labels for the x-axis (columns of the pivoted data).
            y_labels (List[Any]): Labels for the y-axis (index of the pivoted data).
            z_data (pd.DataFrame): The pivoted DataFrame containing the values for the heatmap cells.
                                   Index should be y_labels, columns should be x_labels.
            title (str): Title of the chart.
            x_axis_title (str): Label for the X-axis.
            y_axis_title (str): Label for the Y-axis.
            colorscale (str): Plotly color scale (e.g., 'Viridis', 'Plasma', 'Greens').
            zmin (Optional[float]): Minimum value for colorscale.
            zmax (Optional[float]): Maximum value for colorscale.

        Returns:
            Optional[go.Figure]: A Plotly Figure object, or None if input data is invalid.
        """
        if z_data.empty:
            logger.warning("Input z_data DataFrame for heatmap is empty.")
            return None

        fig = go.Figure(data=go.Heatmap(
            z=z_data.values,
            x=x_labels,
            y=y_labels,
            colorscale=colorscale,
            zmin=zmin,
            zmax=zmax,
            colorbar=dict(title=y_axis_title) # Using y_axis_title for colorbar, can be customized
        ))

        ChartBuilder._apply_default_layout(fig, title, x_axis_title, y_axis_title)
        
        # Ensure axis labels are visible and readable if many categories
        fig.update_xaxes(tickangle=45, tickfont=dict(size=10))
        fig.update_yaxes(tickfont=dict(size=10))

        return fig