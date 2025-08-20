import streamlit as st
from streamlit_plotly_events import plotly_events
import plotly.express as px
import pandas as pd
from scripts.connectors.db_manager import DatabaseManager
from config.logging_config import setup_logging
from dashboard.dashboard_queries import get_chart_data, get_aggregated_data

logger = setup_logging()

st.set_page_config(layout="wide")


@st.cache_data
def load_data(year:int=None, month:int=None):
    """
    Fetches top chart data for albums, artists and tracks
    
    Args:
        year (int): year filter
        month (int): month filter. Only valid if a year filter provided
    
    Returns:
        dict: dict of DataFrames with keys as item types
    """
    with DatabaseManager(logger=logger) as db:
        top_artists = get_chart_data(db, "artist", year, month)
        top_tracks = get_chart_data(db, "track", year, month)
        top_albums = get_chart_data(db, "album", year, month)
        return {
            "artists": top_artists,
            "tracks": top_tracks,
            "albums": top_albums
        }


@st.cache_data
def load_aggregated_data():
    with DatabaseManager(logger=logger) as db:
        monthly_data = get_aggregated_data(db, "monthly")
        yearly_data = get_aggregated_data(db, "yearly")
        all_time_data = get_aggregated_data(db, "all_time")

        monthly_columns = ["year", "month_num", "month_start", "hours_listened", "total_streams", "nonskip_streams", "total_estimated_streams", "distinct tracks", "distinct_artists"]
        yearly_columns = ["year", "year_start", "hours_listened", "total_streams", "nonskip_streams", "total_estimated_streams", "distinct tracks", "distinct_artists"]
        all_time_columns = ["days_listened", "total_streams", "nonskip_streams", "total_estimated_streams", "distinct tracks", "distinct_artists"]

        monthly_df = pd.DataFrame(monthly_data, columns=monthly_columns)
        yearly_df = pd.DataFrame(yearly_data, columns=yearly_columns)
        all_time_df = pd.DataFrame(all_time_data, columns=all_time_columns)
        
        return yearly_df, monthly_df
    

def render_top_chart(df, item_type):
    # section header
    st.subheader(f"Top {item_type.capitalize()}s")

    header_cols = st.columns([1, 3, 1, 1])
    header_cols[1].markdown(f"**{item_type.upper()} NAME**")
    header_cols[2].markdown("**HOURS**", )
    header_cols[3].markdown("**STREAMS**")

    # item list
    for row in df.itertuples():
        # 4 columns: cover art, item title, hours played, estimated streams
        cols = st.columns([1, 3, 1, 1])
        column_height = 85

        with cols[0]:
            st.image(row.cover_art, width=column_height)
        
        title_char_limit = 50 # only allow up to n characters for nicer display
        item_text = row[1] if len(row[1]) < title_char_limit else f"{row[1][:title_char_limit - 3]}..." 
        cols[1].markdown(f"""<div style="font-size:15pt; display: flex; align-items: center; height: {column_height}px;">{item_text}</div>""", unsafe_allow_html=True) 
        cols[2].markdown(f"""<div style="font-size:15pt; display: flex; align-items: center; justify-content: center; height: {column_height}px;">{row.hours_played}</div>""", unsafe_allow_html=True)
        cols[3].markdown(f"""<div style="font-size:15pt; display: flex; align-items: center; justify-content: center; height: {column_height}px;">{row.estimated_streams}</div>""", unsafe_allow_html=True)


def render_stats_section(df, stats_labels):
    cols = st.columns(len(stats_labels))

    for i, col in enumerate(cols):
        value = f"{df.iloc[0][stats_labels[i]]:.1f}"  # format as string with 1 decimal
        label = stats_labels[i].replace("_", " ").upper()
        with col:
            st.metric(label=label, value=value, border=True)

def reset_month():
    st.session_state.selected_month = None


with st.container() as dashboard:
    # load aggregated data
    yearly_df, monthly_df = load_aggregated_data()

    if "selected_month" not in st.session_state:
        st.session_state.selected_month = None # tracks user selected month in the bar chart
        st.write(f"selected month is {st.session_state.selected_month}")

    # get unique years and create a filter by year
    unique_years = [int(year) for year in yearly_df["year"]]
    filter_options = ["All time data"] + unique_years
    year_select = st.selectbox(label="Filter by year", options=filter_options, on_change=reset_month)

    # load chart data based on the year
    top_chart_year = None if year_select == "All time data" else int(year_select)
    top_chart_data = load_data(year=top_chart_year, month=st.session_state.selected_month)

    data_col, top_chart_col = st.columns([2, 1])
    with data_col:
        # Stats section
        with st.container() as stats_section:
            print(st.session_state.selected_month, " ,", year_select)
            if year_select and st.session_state.selected_month:
                selected_data_df = monthly_df[(monthly_df["year"]==int(year_select)) & (monthly_df["month_num"]==st.session_state.selected_month)]
                render_stats_section(selected_data_df, ["hours_listened", "total_streams", "total_estimated_streams"])
        

        # year over year or monthly chart
        with st.container(border=True) as bar_chart:
            if year_select == "All time data":
                yearly_fig = px.bar(yearly_df, x="year", y="hours_listened", orientation="v")
                st.plotly_chart(yearly_fig)
            else:
                print(monthly_df[monthly_df["year"]==year_select].describe())
                monthly_fig = px.bar(monthly_df[monthly_df["year"]==year_select], x="month_num", y="hours_listened", orientation="v", template="plotly_dark")
                # get selected month on click
                click_tracker = plotly_events(monthly_fig)
                if click_tracker:
                    new_month = click_tracker[0]["x"]
                    if new_month != st.session_state.get("selected_month"):
                        st.session_state.selected_month = new_month
                        st.rerun()

    with top_chart_col:
        # load chart data based on the year
        top_chart_year = None if year_select == "All time data" else int(year_select)
        st.write(f"selected year is {top_chart_year}, selected month: {st.session_state.selected_month}")
        top_chart_data = load_data(year=top_chart_year, month=st.session_state.selected_month)
        with st.container(border=True) as top_chart:
            view_choice = st.radio("Switch to:", ["Artists", "Tracks", "Albums"], horizontal=True)
            render_top_chart(top_chart_data[view_choice.lower()], view_choice[:-1].lower())
