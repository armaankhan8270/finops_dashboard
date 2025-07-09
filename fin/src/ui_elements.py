# finops_dashboard/src/ui_elements.py

import streamlit as st
import logging
from typing import Optional

# Import utilities and configuration
from src.utils import handle_errors
from src.config import APP_TITLE, APP_DESCRIPTION, APP_ICON, GLOBAL_CSS, PRIMARY_COLOR, INFO_COLOR, PRIORITY_LEVELS

logger = logging.getLogger(__name__)

class UIElements:
    """
    Provides reusable Streamlit UI components with custom styling.
    """

    @staticmethod
    def set_page_config():
        """
        Sets the global Streamlit page configuration.
        Should be called once at the very beginning of the main app.py.
        """
        try:
            st.set_page_config(
                page_title=APP_TITLE,
                page_icon=APP_ICON,
                layout="wide", # Use wide layout for more content
                initial_sidebar_state="expanded"
            )
            logger.info("Streamlit page configuration set.")
        except Exception as e:
            logger.error(f"Failed to set Streamlit page configuration: {e}", exc_info=True)

    @staticmethod
    def render_global_styles():
        """
        Injects global CSS styles into the Streamlit application.
        This must be called at the start of the app (after set_page_config).
        """
        try:
            st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
            logger.debug("Global CSS styles injected.")
        except Exception as e:
            logger.error(f"Failed to inject global CSS styles: {e}", exc_info=True)


    @staticmethod
    @handle_errors
    def render_page_header(title: str, description: str):
        """
        Renders a large, stylized header for a main dashboard page.
        Uses the 'page-header' CSS class.
        """
        st.markdown(
            f"""
            <div class="page-header">
                <h1>{title}</h1>
                <p>{description}</p>
            </div>
            """,
            unsafe_allow_html=True
        )
        logger.debug(f"Page header '{title}' rendered.")

    @staticmethod
    @handle_errors
    def render_section_header(title: str, icon: Optional[str] = None, description: Optional[str] = None):
        """
        Renders a smaller, stylized header for a section within a page.
        Uses the 'section-header' CSS class.
        """
        st.markdown(
            f"""
            <div class="section-header">
                {f'<span class="icon">{icon}</span>' if icon else ''}
                <h3>{title}</h3>
            </div>
            {f'<p class="section-header-description">{description}</p>' if description else ''}
            """,
            unsafe_allow_html=True
        )
        logger.debug(f"Section header '{title}' rendered.")

    @staticmethod
    @handle_errors
    def render_info_card(header: str, content: str, icon: str = "ℹ️"):
        """
        Renders a custom-styled information card.
        Uses the 'info-card' CSS class.
        """
        st.markdown(
            f"""
            <div class="info-card">
                <div class="info-card-header">
                    <span class="info-card-icon">{icon}</span> {header}
                </div>
                <div class="info-card-content">
                    {content}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        logger.debug(f"Info card '{header}' rendered.")

    @staticmethod
    @handle_errors
    def render_priority_alert(priority_level: str, message: str):
        """
        Renders a custom-styled alert box based on a defined priority level.
        Uses the 'priority-alert' CSS class and colors from PRIORITY_LEVELS in config.
        
        Args:
            priority_level (str): A key from src.config.PRIORITY_LEVELS (e.g., "High Priority").
            message (str): The message content for the alert.
        """
        # Get styling from config based on priority_level
        style = PRIORITY_LEVELS.get(priority_level, PRIORITY_LEVELS["N/A"])
        
        bg_color = style['bg_color']
        text_color = style['text_color']
        font_weight = style['font_weight']
        icon = style['icon']
        label = style['label'] # Use the full label from config for the header

        st.markdown(
            f"""
            <div class="priority-alert" style="background-color: {bg_color}; color: {text_color}; border-color: {text_color};">
                <span class="priority-icon" style="color: {text_color};">{icon}</span>
                <div class="priority-content">
                    <strong style="font-weight: {font_weight};">{label}</strong><br>
                    {message}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        logger.debug(f"Priority alert for '{priority_level}' rendered.")