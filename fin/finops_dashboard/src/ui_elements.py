# finops_dashboard/src/ui_elements.py

import streamlit as st
from typing import Optional, Dict, Any, Literal
import logging

# Import constants from config.py for styling and app metadata
from src.config import (
    APP_TITLE, APP_ICON, APP_DESCRIPTION, PRIMARY_COLOR,
    GLOBAL_CSS, PRIORITY_LEVELS
)
from src.utils import handle_errors # For robust UI elements

logger = logging.getLogger(__name__)

class UIElements:
    """
    Provides reusable, custom-styled Streamlit UI components
    to ensure a consistent look and feel across the dashboard.
    """

    @staticmethod
    def set_page_config() -> None:
        """
        Sets the global Streamlit page configuration (title, icon, layout).
        This should be called once at the very beginning of app.py.
        """
        try:
            st.set_page_config(
                page_title=APP_TITLE,
                page_icon=APP_ICON,
                layout="wide", # Use wide layout by default
                initial_sidebar_state="expanded"
            )
            logger.info("Streamlit page config set.")
        except Exception as e:
            logger.error(f"Failed to set page config: {e}", exc_info=True)
            # This error might prevent app from loading, so just log and continue if possible.

    @staticmethod
    def render_global_styles() -> None:
        """
        Injects global custom CSS into the Streamlit application.
        This allows for centralized styling of Streamlit components and custom HTML.
        """
        try:
            st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
            logger.info("Global CSS styles injected.")
        except Exception as e:
            logger.error(f"Failed to inject global CSS: {e}", exc_info=True)
            st.warning("Could not load custom styles. Dashboard may look unformatted.")


    @staticmethod
    @handle_errors
    def render_page_header(title: str, description: str) -> None:
        """
        Renders a stylized header for the main page.

        Args:
            title (str): The main title for the page.
            description (str): A brief description of the page's content.
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
        logger.debug(f"Rendered page header: {title}")

    @staticmethod
    @handle_errors
    def render_section_header(title: str, icon: Optional[str] = None, description: Optional[str] = None) -> None:
        """
        Renders a stylized header for a section within a page.

        Args:
            title (str): The title for the section.
            icon (Optional[str]): An emoji or icon string to display next to the title.
            description (Optional[str]): An optional brief description for the section.
        """
        icon_html = f'<span class="icon">{icon}</span>' if icon else ''
        st.markdown(
            f"""
            <div class="section-header">
                {icon_html}
                <h3>{title}</h3>
            </div>
            """,
            unsafe_allow_html=True
        )
        if description:
            st.markdown(f'<p class="section-header-description">{description}</p>', unsafe_allow_html=True)
        logger.debug(f"Rendered section header: {title}")

    @staticmethod
    @handle_errors
    def render_info_card(header: str, content: str, icon: Optional[str] = "ℹ️") -> None:
        """
        Renders a stylized informational card.

        Args:
            header (str): The bold header for the info card.
            content (str): The main text content of the info card.
            icon (Optional[str]): An emoji or icon to display at the top left of the card.
        """
        icon_html = f'<span class="info-card-icon">{icon}</span>' if icon else ''
        st.markdown(
            f"""
            <div class="info-card">
                <div class="info-card-header">{icon_html} {header}</div>
                <div class="info-card-content">{content}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
        logger.debug(f"Rendered info card: {header}")

    @staticmethod
    @handle_errors
    def render_priority_alert(
        priority_level_key: Literal["High Priority", "Medium Priority", "Low Priority", "Normal", "N/A"],
        message_title: str,
        message_content: str
    ) -> None:
        """
        Renders a stylized alert box based on a predefined priority level.
        Colors and icons are dynamically pulled from config.PRIORITY_LEVELS.

        Args:
            priority_level_key (Literal[...]): Key corresponding to a priority level in config.PRIORITY_LEVELS.
            message_title (str): The main title/summary of the alert.
            message_content (str): Detailed content of the alert.
        """
        priority_config = PRIORITY_LEVELS.get(priority_level_key, PRIORITY_LEVELS["N/A"])
        icon = priority_config["icon"]
        bg_color = priority_config["bg_color"]
        text_color = priority_config["text_color"]
        font_weight = priority_config["font_weight"]

        st.markdown(
            f"""
            <div class="priority-alert" style="background-color: {bg_color}; border-color: {text_color};">
                <span class="priority-icon" style="color: {text_color};">{icon}</span>
                <div class="priority-content" style="color: {text_color}; font-weight: {font_weight};">
                    <strong>{message_title}</strong>
                    {message_content}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        logger.debug(f"Rendered priority alert: {message_title} ({priority_level_key})")