import streamlit as st
st.set_page_config(layout="wide")
import pandas as pd
from snowflake.snowpark import FileOperation
from connection_snowflake.connection import getSession, runQuery
import json
from datetime import datetime
from login.login import login
import time
from st_pages import Page, add_page_title, show_pages
from streamlit_option_menu import option_menu
from upload_nf import main as upload_nf
from relatorio import main as relatorio

session = getSession()


col1, col2, col3, col4, col5 = st.columns(5)

if 'connection_established' not in st.session_state or not st.session_state.connection_established:
    with col3:
        login(session)

elif 'connection_established' in st.session_state:
    with st.sidebar:
        selected = option_menu("", ["Carga Nota", "Relatório"],
                               icons=['📈', '📈'], menu_icon="'📈'", default_index=0,
                            #    styles={
                            #     "nav-link-selected": {"background-color": "#105aff"},
                            #     }
        )
    
    if selected == "Carga Nota":
        upload_nf(session)

    if selected == "Relatório":
        relatorio()

    if st.button('Sair'):
        time.sleep(1)
        st.session_state.clear()
        st.cache_data.clear()
        st.rerun()
