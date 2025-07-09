# finops_dashboard/src/chart_builder.py

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import logging
from typing import Optional, List, Dict, Any, Union

# Import utilities and configuration
from src.utils import handle_errors
from src.config import PRIMARY_COLOR, ACCENT_COLOR_SCHEME, PLOTLY_LAYOUT_DEFAULTS

logger = logging.getLogger(__name__)

class ChartBuilder:
    """
    Responsible for generating various Plotly charts from DataFrames.
    This class does NOT fetch or process data; it only visualizes it.
    """

    @staticmethod
    def _apply_default_layout(fig: go.Figure, title: str, y_axis_title: str = "", x_axis_title: str = ""):
        """Applies consistent default layout settings to a Plotly figure."""
        fig.update_layout(
            title_text=f"<b>{title}</b>",
            title_x=0.05, # Align title to left
            title_font_size=PLOTLY_LAYOUT_DEFAULTS["title_font_size"],
            title_font_color=PLOTLY_LAYOUT_DEFAULTS["title_font_color"],
            plot_bgcolor=PLOTLY_LAYOUT_DEFAULTS["plot_bgcolor"],
            paper_bgcolor=PLOTLY_LAYOUT_DEFAULTS["paper_bgcolor"],
            font=PLOTLY_LAYOUT_DEFAULTS["font"],
            margin=PLOTLY_LAYOUT_DEFAULTS["margin"],
            height=PLOTLY_LAYOUT_DEFAULTS["height"],
            xaxis_title=x_axis_title,
            yaxis_title=y_axis_title,
            xaxis=PLOTLY_LAYOUT_DEFAULTS["xaxis"],
            yaxis=PLOTLY_LAYOUT_DEFAULTS["yaxis"],
            legend=PLOTLY_LAYOUT_DEFAULTS["legend"],
            hovermode="x unified", # Shows tooltip for all series at a given x-value
            colorway=ACCENT_COLOR_SCHEME # Apply consistent color scheme
        )
        # Ensure axis lines are visible by setting their color explicitly
        fig.update_xaxes(showline=True, linewidth=1, linecolor='lightgray')
        fig.update_yaxes(showline=True, linewidth=1, linecolor='lightgray')


    @staticmethod
    @handle_errors
    def build_line_chart(
        df: pd.DataFrame, 
        x_col: str, 
        y_col: str, 
        title: str, 
        x_axis_title: str = "", 
        y_axis_title: str = "",
        line_color: str = PRIMARY_COLOR,
        hover_name_col: Optional[str] = None # For better hover info if needed
    ) -> Optional[go.Figure]:
        """
        Builds a basic line chart.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            x_col (str): The column to use for the x-axis.
            y_col (str): The column to use for the y-axis.
            title (str): The title of the chart.
            x_axis_title (str): The title for the x-axis.
            y_axis_title (str): The title for the y-axis.
            line_color (str): Color of the line.
            hover_name_col (str, optional): Column to display as name on hover.

        Returns:
            Optional[go.Figure]: A Plotly Figure object or None if an error occurs.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for line chart: {title}")
            return None

        # Ensure columns exist
        if not all(col in df.columns for col in [x_col, y_col]):
            logger.error(f"Missing required columns for line chart: {x_col}, {y_col}")
            return None
            
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='lines+markers', # Show both lines and markers
            name=y_col.replace("_", " ").title(), # Default name in legend
            line=dict(color=line_color, width=3),
            marker=dict(size=6, color=line_color, line=dict(width=1, color='DarkSlateGrey')),
            hoverinfo='x+y+name',
            hoverlabel=dict(bgcolor='white', font_size=12, namelength=-1),
            hovertemplate=
                "<b>Date</b>: %{x}<br>" +
                f"<b>{y_axis_title}</b>: %{{y:.2f}}<extra></extra>" # Format y for currency etc.
        ))

        ChartBuilder._apply_default_layout(fig, title, y_axis_title, x_axis_title)
        
        # Adjust x-axis for dates
        if pd.api.types.is_datetime64_any_dtype(df[x_col]):
            fig.update_xaxes(
                tickformat="%b %d",
                rangeselector=dict(
                    buttons=list([
                        dict(count=7, label="7d", step="day", stepmode="backward"),
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=True),
                type="date"
            )
        
        return fig

    @staticmethod
    @handle_errors
    def build_bar_chart(
        df: pd.DataFrame, 
        x_col: str, 
        y_col: str, 
        title: str, 
        x_axis_title: str = "", 
        y_axis_title: str = "",
        orientation: str = 'v', # 'v' for vertical, 'h' for horizontal
        color_col: Optional[str] = None # Column to use for coloring bars
    ) -> Optional[go.Figure]:
        """
        Builds a basic bar chart.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            x_col (str): The column for the x-axis.
            y_col (str): The column for the y-axis.
            title (str): The title of the chart.
            x_axis_title (str): The title for the x-axis.
            y_axis_title (str): The title for the y-axis.
            orientation (str): 'v' for vertical bars, 'h' for horizontal bars.
            color_col (str, optional): Column to use for coloring bars (e.g., 'PRIORITY_LEVEL').

        Returns:
            Optional[go.Figure]: A Plotly Figure object or None if an error occurs.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for bar chart: {title}")
            return None

        required_cols = [x_col, y_col]
        if color_col:
            required_cols.append(color_col)
        if not all(col in df.columns for col in required_cols):
            logger.error(f"Missing required columns for bar chart: {', '.join([col for col in required_cols if col not in df.columns])}")
            return None

        # Use Plotly Express for simpler bar charts, especially with color
        if color_col and color_col in df.columns:
            # Custom sorting for PRIORITY_LEVEL if it's the color_col
            if color_col == 'PRIORITY_LEVEL' and all(level in df[color_col].unique() for level in PRIORITY_LEVELS.keys()):
                # Create a mapping for sorting
                priority_order = list(PRIORITY_LEVELS.keys())
                df[color_col] = pd.Categorical(df[color_col], categories=priority_order, ordered=True)
                df = df.sort_values(by=color_col)

                # Custom colors for priority levels
                color_map = {level: PRIORITY_LEVELS[level]['text_color'] for level in PRIORITY_LEVELS.keys()}
                color_discrete_map = {k: v for k, v in color_map.items() if k in df[color_col].unique()}
                
                fig = px.bar(
                    df, 
                    x=x_col, 
                    y=y_col, 
                    color=color_col, 
                    orientation=orientation,
                    color_discrete_map=color_discrete_map,
                    text_auto='.2s' if 'credits' in y_col.lower() else False, # Auto text for credits
                    hover_name=x_col,
                    hover_data={x_col: True, y_col: ':.2f', color_col: True}
                )
            else:
                fig = px.bar(
                    df, 
                    x=x_col, 
                    y=y_col, 
                    color=color_col, 
                    orientation=orientation,
                    color_discrete_sequence=ACCENT_COLOR_SCHEME,
                    text_auto='.2s' if 'credits' in y_col.lower() else False,
                    hover_name=x_col,
                    hover_data={x_col: True, y_col: ':.2f', color_col: True}
                )
        else:
            fig = px.bar(
                df, 
                x=x_col, 
                y=y_col, 
                orientation=orientation,
                color_discrete_sequence=[PRIMARY_COLOR], # Use primary color for single-color bars
                text_auto='.2s' if 'credits' in y_col.lower() else False,
                hover_name=x_col,
                hover_data={x_col: True, y_col: ':.2f'}
            )
            
        ChartBuilder._apply_default_layout(fig, title, y_axis_title, x_axis_title)
        
        # Adjust for horizontal bar chart
        if orientation == 'h':
            fig.update_yaxes(autorange="reversed") # To show highest value at top for horizontal bars
            # Swap titles for horizontal chart
            fig.update_layout(xaxis_title=y_axis_title, yaxis_title=x_axis_title)

        return fig

    @staticmethod
    @handle_errors
    def build_pie_chart(
        df: pd.DataFrame, 
        names_col: str, 
        values_col: str, 
        title: str, 
        hole: float = 0.4 # For a donut chart
    ) -> Optional[go.Figure]:
        """
        Builds a pie/donut chart.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            names_col (str): The column to use for slice labels (names).
            values_col (str): The column to use for slice sizes (values).
            title (str): The title of the chart.
            hole (float): Value between 0 and 1 for the size of the hole in a donut chart. 0 for a regular pie chart.

        Returns:
            Optional[go.Figure]: A Plotly Figure object or None if an error occurs.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for pie chart: {title}")
            return None

        if not all(col in df.columns for col in [names_col, values_col]):
            logger.error(f"Missing required columns for pie chart: {names_col}, {values_col}")
            return None

        # Ensure values column is numeric
        df[values_col] = pd.to_numeric(df[values_col], errors='coerce').fillna(0)

        # Filter out zero values to prevent errors or empty slices
        df_filtered = df[df[values_col] > 0].copy()

        if df_filtered.empty:
            logger.warning(f"No non-zero values after filtering for pie chart: {title}")
            return None

        fig = go.Figure(data=[go.Pie(
            labels=df_filtered[names_col], 
            values=df_filtered[values_col], 
            hole=hole,
            marker_colors=ACCENT_COLOR_SCHEME,
            hoverinfo="label+percent+value",
            textinfo="percent+label", # Show percentage and label on slices
            textfont_size=12,
            pull=[0.05 if df_filtered[values_col].iloc[i] == df_filtered[values_col].max() else 0 for i in range(len(df_filtered))] # Pull out largest slice
        )])
        
        ChartBuilder._apply_default_layout(fig, title)
        fig.update_traces(marker=dict(line=dict(color='#000000', width=1))) # Add thin black border to slices
        
        # Don't show legend if there are too many slices (e.g., > 10)
        if len(df_filtered) > 10:
            fig.update_layout(showlegend=False)

        return fig

    @staticmethod
    @handle_errors
    def build_heatmap(
        df: pd.DataFrame, 
        x_labels: Union[List[str], pd.Series], # Can be hours, days etc.
        y_labels: Union[List[str], pd.Series], # Can be users, warehouses etc.
        z_data: pd.DataFrame, # The pivoted data for the heatmap
        title: str, 
        x_axis_title: str = "", 
        y_axis_title: str = "",
        colorscale: str = 'Viridis', # or 'Plasma', 'Portland', 'Greens'
        show_values_on_hover: bool = True
    ) -> Optional[go.Figure]:
        """
        Builds a heatmap. Assumes z_data is already a pivoted DataFrame.

        Args:
            df (pd.DataFrame): The original (unpivoted) DataFrame used to extract x/y labels if needed.
                               Not directly used for z_data but good for context/column names.
            x_labels (Union[List[str], pd.Series]): Labels for the x-axis (columns of the pivoted data).
            y_labels (Union[List[str], pd.Series]): Labels for the y-axis (index of the pivoted data).
            z_data (pd.DataFrame): The pivoted DataFrame containing the values for the heatmap cells.
                                   Index should correspond to y_labels, columns to x_labels.
            title (str): The title of the chart.
            x_axis_title (str): The title for the x-axis.
            y_axis_title (str): The title for the y-axis.
            colorscale (str): Plotly color scale name (e.g., 'Viridis', 'Plasma').
            show_values_on_hover (bool): Whether to show values on hover.

        Returns:
            Optional[go.Figure]: A Plotly Figure object or None if an error occurs.
        """
        if z_data.empty:
            logger.warning(f"Empty pivoted DataFrame provided for heatmap: {title}")
            return None

        # Convert z_data to numpy array for Plotly heatmap trace
        z_values = z_data.values

        fig = go.Figure(data=go.Heatmap(
            z=z_values,
            x=x_labels, # Assuming x_labels are column names of z_data
            y=y_labels, # Assuming y_labels are index names of z_data
            colorscale=colorscale,
            colorbar=dict(title=y_axis_title), # Or something more descriptive for the colorbar
            hovertemplate=
                f"<b>{x_axis_title}</b>: %{{x}}<br>" +
                f"<b>{y_axis_title}</b>: %{{y}}<br>" +
                "<b>Value</b>: %{z:.2f}<extra></extra>"
        ))
        
        ChartBuilder._apply_default_layout(fig, title, y_axis_title, x_axis_title)
        
        # Ensure x-axis for hours are integers
        if all(isinstance(x, int) for x in x_labels):
            fig.update_xaxes(type='category') # Treat hours as categories, not continuous numbers

        return fig