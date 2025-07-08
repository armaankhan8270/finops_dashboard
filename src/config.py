# finops_dashboard/src/config.py

from datetime import datetime, timedelta

# --- Application Title and Description ---
APP_TITLE = "Snowflake FinOps Dashboard"
APP_DESCRIPTION = "Optimize your Snowflake costs and gain usage insights."
APP_ICON = "❄️" # For page icon

# --- Theme and Styling Colors ---
PRIMARY_COLOR = "#1f77b4"  # A pleasant blue, consistent with Streamlit's default blues
SECONDARY_COLOR = "#667eea" # A complementary purple/blue
GRADIENT_START = "#667eea"
GRADIENT_END = "#764ba2"
TEXT_COLOR = "#333333"     # Dark grey for readability
BACKGROUND_COLOR = "#f0f2f6" # Light grey for app background
COMPONENT_BACKGROUND_COLOR = "#ffffff" # White for cards/widgets

SUCCESS_COLOR = "#28a745"   # Green
WARNING_COLOR = "#ffc107"   # Orange
DANGER_COLOR = "#dc3545"    # Red
INFO_COLOR = "#17a2b8"      # Teal/Blue

# A diverse color scheme for charts
ACCENT_COLOR_SCHEME = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
]


# --- Date Ranges for Filters ---
# Key: Display name, Value: Number of days back (0 for custom range)
DATE_RANGES = {
    "Last 7 Days": 7,
    "Last 30 Days": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180,
    "Last 9 Months": 270,
    "Last 1 Year": 365,
    "Custom Range": 0, # Sentinel value for custom date input
}
DEFAULT_DATE_RANGE_INDEX = 1 # "Last 30 Days"

# --- Metric Card Configuration ---
# Define how different types of metrics should be formatted
METRIC_FORMATS = {
    "currency": {"prefix": "$", "suffix": "", "decimals": 2, "thousands_sep": True},
    "percentage": {"prefix": "", "suffix": "%", "decimals": 1, "thousands_sep": False},
    "duration_seconds": {"prefix": "", "suffix": "s", "decimals": 1, "thousands_sep": False},
    "duration_ms": {"prefix": "", "suffix": "ms", "decimals": 0, "thousands_sep": True},
    "number": {"prefix": "", "suffix": "", "decimals": 0, "thousands_sep": True}, # Default for integers
    "float_number": {"prefix": "", "suffix": "", "decimals": 2, "thousands_sep": True}, # For floats with 2 decimal places
}

# --- Priority Level Definitions (for identifying High-Impact Users, Optimization opportunities) ---
# Used by data_processor and ui_elements
PRIORITY_LEVELS = {
    "High Priority": {
        "label": "High Priority: >2x Avg Cost 🔴",
        "bg_color": "#ffebee", # Light red background
        "text_color": "#c62828", # Dark red text
        "font_weight": "bold",
        "icon": "🚨"
    },
    "Medium Priority": {
        "label": "Medium Priority: >1.5x Avg Cost 🟠",
        "bg_color": "#fff3e0", # Light orange background
        "text_color": "#ef6c00", # Dark orange text
        "font_weight": "500",
        "icon": "⚠️"
    },
    "Above Avg Cost": {
        "label": "Above Avg Cost 🟡",
        "bg_color": "#fffde7", # Light yellow background
        "text_color": "#f57f17", # Dark yellow text
        "font_weight": "normal",
        "icon": "📈"
    },
    "Good Performance": {
        "label": "Good Performance 🟢",
        "bg_color": "#e8f5e8", # Light green background
        "text_color": "#388e3c", # Dark green text
        "font_weight": "normal",
        "icon": "✅"
    },
    "N/A": { # For cases where priority cannot be determined
        "label": "N/A",
        "bg_color": "#e0e0e0",
        "text_color": "#666666",
        "font_weight": "normal",
        "icon": "➖"
    }
}

# --- Plotly Chart Defaults ---
PLOTLY_LAYOUT_DEFAULTS = {
    "height": 400,
    "title_font_size": 18,
    "title_font_color": PRIMARY_COLOR,
    "plot_bgcolor": "rgba(0,0,0,0)", # Transparent plot background
    "paper_bgcolor": "rgba(0,0,0,0)", # Transparent paper background
    "font": {"family": "Arial, sans-serif", "size": 12, "color": TEXT_COLOR},
    "margin": {"l": 20, "r": 20, "t": 60, "b": 20}, # Adjust margins for better fit
    "xaxis": {"showgrid": True, "gridcolor": "rgba(0,0,0,0.1)", "linewidth": 1, "linecolor": "rgba(0,0,0,0.2)"},
    "yaxis": {"showgrid": True, "gridcolor": "rgba(0,0,0,0.1)", "linewidth": 1, "linecolor": "rgba(0,0,0,0.2)"},
    "legend": {"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1} # Horizontal legend at top right
}

# --- Global HTML Styles (for st.markdown to inject custom CSS) ---
# These styles apply across the app and customize Streamlit's default appearance
GLOBAL_CSS = f"""
    <style>
    /* Main container padding */
    .reportview-container .main .block-container {{
        padding-top: 2rem;
        padding-right: 2rem;
        padding-left: 2rem;
        padding-bottom: 2rem;
    }}
    
    /* Streamlit App Background */
    .stApp {{
        background-color: {BACKGROUND_COLOR};
        color: {TEXT_COLOR};
    }}

    /* Customizing st.metric appearance */
    .stMetric {{
        background-color: {COMPONENT_BACKGROUND_COLOR};
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08); /* Soft shadow */
        border-left: 5px solid {PRIMARY_COLOR}; /* Accent border */
        height: 120px; /* Fixed height for consistency */
        display: flex;
        flex-direction: column;
        justify-content: center;
        transition: transform 0.2s ease-in-out; /* Smooth hover effect */
    }}
    .stMetric:hover {{
        transform: translateY(-3px); /* Lift on hover */
    }}
    .stMetric > div:first-child {{ /* Metric label */
        display: flex;
        align-items: center;
        margin-bottom: 8px;
        font-size: 14px;
        color: #666;
        font-weight: 500;
    }}
    .stMetric > div:first-child > div:first-child {{ /* Icon */
        font-size: 20px;
        margin-right: 8px;
    }}
    .stMetric > div:nth-child(2) {{ /* Metric value */
        font-size: 28px;
        font-weight: 700;
        color: {PRIMARY_COLOR};
        margin-bottom: 4px;
    }}
    .stMetric > div:nth-child(3) {{ /* Metric delta */
        font-size: 14px;
        margin-top: 4px;
    }}

    /* Custom Info Card */
    .info-card {{
        border: 1px solid {INFO_COLOR}20;
        border-radius: 8px;
        padding: 12px;
        margin: 8px 0;
        background: linear-gradient(135deg, {INFO_COLOR}08, {INFO_COLOR}05);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }}
    .info-card-header {{
        display: flex;
        align-items: center;
        margin-bottom: 8px;
        font-weight: 600;
        color: {INFO_COLOR};
    }}
    .info-card-icon {{
        margin-right: 8px;
        font-size: 16px;
    }}
    .info-card-content {{
        color: #444;
        font-size: 14px;
        line-height: 1.4;
    }}

    /* Section Header */
    .section-header {{
        margin: 30px 0 20px 0;
        padding: 15px 0;
        border-bottom: 2px solid #e0e0e0;
        display: flex;
        align-items: center;
        gap: 10px; /* Space between icon and text */
    }}
    .section-header h3 {{
        color: {PRIMARY_COLOR};
        margin: 0;
        font-size: 24px;
        font-weight: 600;
    }}
    .section-header p {{
        color: #666;
        margin: 8px 0 0 0;
        font-size: 14px;
    }}
    .section-header .icon {{
        font-size: 28px;
        color: {PRIMARY_COLOR};
    }}

    /* Page Header */
    .page-header {{
        background: linear-gradient(135deg, {GRADIENT_START} 0%, {GRADIENT_END} 100%);
        padding: 30px;
        border-radius: 12px;
        margin-bottom: 30px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }}
    .page-header h1 {{
        margin: 0;
        font-size: 36px;
        font-weight: 700;
        color: white; /* Override h1 color */
    }}
    .page-header p {{
        margin: 10px 0 0 0;
        font-size: 18px;
        opacity: 0.9;
    }}

    /* Priority Alert (uses config.PRIORITY_LEVELS for colors) */
    .priority-alert {{
        padding: 15px;
        border-radius: 8px;
        margin: 15px 0;
        font-weight: 500;
        border: 1px solid; /* Border to match text color */
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        display: flex;
        align-items: flex-start;
        gap: 10px;
    }}
    .priority-alert .priority-icon {{
        font-size: 20px;
        line-height: 1; /* Align icon vertically */
    }}
    .priority-alert .priority-content {{
        flex-grow: 1;
    }}
    .priority-alert strong {{
        font-size: 16px;
    }}


    /* Custom column spacing for selectboxes/date inputs */
    .st-dg, .st-dh {{ /* Targets the parent divs of st.selectbox/st.date_input */
        margin-bottom: 1rem; /* Add some space below filters */
    }}

    </style>
"""

# --- Database Schema (adjust as per your Snowflake setup) ---
# These are used by data_fetcher to construct queries,
# ensuring consistency and easy updates if your schema paths change.
SNOWFLAKE_ACCOUNT_USAGE_SCHEMA = "SNOWFLAKE.ACCOUNT_USAGE"
QUERY_HISTORY_TABLE = f"{SNOWFLAKE_ACCOUNT_USAGE_SCHEMA}.QUERY_HISTORY"
METERING_HISTORY_TABLE = f"{SNOWFLAKE_ACCOUNT_USAGE_SCHEMA}.METERING_HISTORY"
LOGIN_HISTORY_TABLE = f"{SNOWFLAKE_ACCOUNT_USAGE_SCHEMA}.LOGIN_HISTORY"
WAREHOUSE_METERING_HISTORY_TABLE = f"{SNOWFLAKE_ACCOUNT_USAGE_SCHEMA}.WAREHOUSE_METERING_HISTORY"