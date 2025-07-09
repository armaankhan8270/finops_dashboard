# finops_dashboard/src/config.py

import logging
from typing import Dict, Any, List

# --- 1. Application Metadata ---
APP_TITLE: str = "Snowflake FinOps Dashboard"
APP_DESCRIPTION: str = "Gain insights into Snowflake credit consumption, query performance, and user activity for cost optimization and better resource management."
APP_ICON: str = "❄️" # A simple emoji for the browser tab icon

# --- 2. Snowflake Account Usage Table Names ---
# These are standard views provided by Snowflake's ACCOUNT_USAGE schema.
# Ensure your Snowflake role has access to these views.
# Example: GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE YOUR_ROLE;
QUERY_HISTORY_TABLE: str = "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
METERING_HISTORY_TABLE: str = "SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY"
LOGIN_HISTORY_TABLE: str = "SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY"
WAREHOUSE_METERING_HISTORY_TABLE: str = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY"
# Add other tables as needed, e.g., DATABASE_STORAGE_USAGE_HISTORY, etc.

# --- 3. Styling & Theming ---
# Define your primary brand color. This will be used for headers, primary buttons, etc.
PRIMARY_COLOR: str = "#0077B6" # A shade of blue (Snowflake blue)

# Accent colors for charts and other UI elements
ACCENT_COLOR_SCHEME: List[str] = [
    "#0077B6", # Primary Blue
    "#00B4D8", # Lighter Blue
    "#48CAE0", # Even Lighter Blue
    "#0096C7", # Darker Blue
    "#90E0EF", # Lightest Blue
    "#ADE8F4", # Pale Blue
    "#CAF0F8", # Very Pale Blue
]

# Specific colors for info cards, warnings, errors, etc.
INFO_COLOR: str = "#e0f7fa"    # Light cyan for informational messages
SUCCESS_COLOR: str = "#e8f5e9" # Light green for success messages
WARNING_COLOR: str = "#fffde7" # Light yellow for warnings
ERROR_COLOR: str = "#ffebee"   # Light red for errors

# Priority Level Definitions (for alerts, color-coding users, etc.)
# Each level has a label, icon, background color, text color, and font weight.
PRIORITY_LEVELS: Dict[str, Dict[str, str]] = {
    "High Priority": {
        "label": "High Priority",
        "icon": "🔴", # Red circle
        "bg_color": "#FFEBEE", # Light red background
        "text_color": "#C62828", # Dark red text
        "font_weight": "bold"
    },
    "Medium Priority": {
        "label": "Medium Priority",
        "icon": "🟠", # Orange circle
        "bg_color": "#FFF3E0", # Light orange background
        "text_color": "#EF6C00", # Dark orange text
        "font_weight": "bold"
    },
    "Low Priority": {
        "label": "Low Priority",
        "icon": "🟢", # Green circle
        "bg_color": "#E8F5E9", # Light green background
        "text_color": "#2E7D32", # Dark green text
        "font_weight": "normal"
    },
    "Normal": {
        "label": "Normal",
        "icon": "⚪", # White circle
        "bg_color": "#F5F5F5", # Light gray background
        "text_color": "#616161", # Gray text
        "font_weight": "normal"
    },
    "N/A": { # For cases where priority isn't applicable or undefined
        "label": "N/A",
        "icon": "⚫", # Black circle
        "bg_color": "#E0E0E0", # Gray background
        "text_color": "#424242", # Dark gray text
        "font_weight": "normal"
    }
}


# --- 4. Plotly Chart Defaults ---
PLOTLY_LAYOUT_DEFAULTS: Dict[str, Any] = {
    "font_family": "Roboto, sans-serif", # Preferred font
    "title_font_size": 20,
    "title_x": 0.05, # Align title to left
    "xaxis_title_font_size": 14,
    "yaxis_title_font_size": 14,
    "hovermode": "x unified", # Shows all traces at a specific X-value
    "template": "plotly_white", # Clean white background
    "colorway": ACCENT_COLOR_SCHEME, # Use our defined color scheme
    "margin": {"l": 50, "r": 50, "t": 80, "b": 50}, # Adjust margins
}

# --- 5. Metric Formatting Rules ---
# Defines how different types of metrics should be displayed (suffix, prefix, decimals)
METRIC_FORMATS: Dict[str, Dict[str, Any]] = {
    "number": {"prefix": "", "suffix": "", "decimals": 0},
    "float_number": {"prefix": "", "suffix": "", "decimals": 2}, # For credits
    "percentage": {"prefix": "", "suffix": "%", "decimals": 1},
    "currency": {"prefix": "$", "suffix": "", "decimals": 2},
    "duration_seconds": {"prefix": "", "suffix": "s", "decimals": 1} # For time in seconds
}

# --- 6. Logging Configuration ---
LOGGING_LEVEL: int = logging.INFO # Set to logging.DEBUG for more verbose output during development
LOGGING_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# --- 7. Custom CSS for Streamlit Components ---
# This CSS is injected globally to customize Streamlit's default appearance.
# It defines styles for custom classes used by UIElements, st.metric, etc.
GLOBAL_CSS: str = f"""
<style>
    /* Global styles for the app */
    body {{
        font-family: 'Roboto', sans-serif;
        color: #333;
        background-color: #f0f2f6; /* Light gray background */
    }}

    /* Page Header styling (used by UIElements.render_page_header) */
    .page-header {{
        background: linear-gradient(to right, {PRIMARY_COLOR}, {ACCENT_COLOR_SCHEME[1]});
        padding: 30px 40px;
        border-radius: 10px;
        margin-bottom: 30px;
        color: white;
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.1);
    }}
    .page-header h1 {{
        color: white;
        font-size: 2.5em;
        margin-bottom: 5px;
    }}
    .page-header p {{
        color: #e0e0e0;
        font-size: 1.1em;
    }}

    /* Section Header styling (used by UIElements.render_section_header) */
    .section-header {{
        display: flex;
        align-items: center;
        margin-top: 30px;
        margin-bottom: 15px;
        padding-bottom: 5px;
        border-bottom: 2px solid {PRIMARY_COLOR};
    }}
    .section-header .icon {{
        font-size: 1.5em;
        margin-right: 10px;
    }}
    .section-header h3 {{
        color: {PRIMARY_COLOR};
        font-size: 1.8em;
        margin: 0;
    }}
    .section-header-description {{
        color: #616161;
        font-style: italic;
        margin-top: -10px;
        margin-bottom: 20px;
    }}

    /* Custom Info Card styling (used by UIElements.render_info_card) */
    .info-card {{
        background-color: {INFO_COLOR};
        border-left: 5px solid {PRIMARY_COLOR};
        border-radius: 8px;
        padding: 15px 20px;
        margin-bottom: 20px;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
    }}
    .info-card-header {{
        font-weight: bold;
        color: {PRIMARY_COLOR};
        margin-bottom: 5px;
        display: flex;
        align-items: center;
    }}
    .info-card-icon {{
        margin-right: 8px;
        font-size: 1.2em;
    }}
    .info-card-content {{
        color: #555;
        font-size: 0.95em;
    }}

    /* Custom Priority Alert styling (used by UIElements.render_priority_alert) */
    .priority-alert {{
        border-radius: 8px;
        padding: 15px 20px;
        margin-bottom: 15px;
        display: flex;
        align-items: flex-start;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
        border: 1px solid; /* Border color will be set by JS via inline style */
    }}
    .priority-alert .priority-icon {{
        font-size: 1.8em;
        margin-right: 15px;
        line-height: 1;
    }}
    .priority-alert .priority-content {{
        flex-grow: 1;
        font-size: 0.95em;
    }}
    .priority-alert .priority-content strong {{
        display: block; /* Ensures the strong tag takes full width */
        margin-bottom: 4px;
        font-size: 1.1em;
    }}

    /* Adjustments for st.metric to apply custom styling */
    /* This targets the value, label, and delta components of st.metric */
    [data-testid="stMetricValue"] {{
        font-size: 2.5em !important;
        color: {PRIMARY_COLOR} !important;
    }}
    [data-testid="stMetricLabel"] {{
        font-size: 1.1em !important;
        color: #555 !important;
    }}
    /* Delta values (green, red, gray) */
    [data-testid="stMetricDelta"] svg {{
        width: 1.5em; /* Adjust arrow size */
        height: 1.5em;
    }}
    /* Specific delta colors based on config for better control */
    .delta-color-green [data-testid="stMetricDelta"] {{ color: #2E7D32 !important; }} /* Dark green */
    .delta-color-red [data-testid="stMetricDelta"] {{ color: #C62828 !important; }} /* Dark red */
    .delta-color-gray [data-testid="stMetricDelta"] {{ color: #616161 !important; }} /* Gray */

</style>
"""