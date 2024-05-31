import streamlit as st
st.set_page_config(layout="wide")
from connection_snowflake.connection import getSession
from login.login import login
import time
from streamlit_option_menu import option_menu
from upload_nf import main as upload_nf
from relatorio import main as relatorio

session = getSession()

col1, col2, col3, col4, col5 = st.columns(5)

if 'connection_established' not in st.session_state or not st.session_state.connection_established:
    with col3:
        login(session)

elif 'connection_established' in st.session_state:
    if st.session_state.user_id != 201:
        with st.sidebar:
            # logo fgv
            selected = option_menu("Menu", ["Carga Nota", "Relatório"],
                                icons=['📈', '📈'], menu_icon="'📈'", default_index=0,
                                #    styles={
                                #     "nav-link-selected": {"background-color": "#105aff"},
                                #     }
            )

            col1, col2, col3 = st.columns(3)

            if col2.button('Sair'):
                time.sleep(1)
                st.session_state.clear()
                st.cache_data.clear()
                st.rerun()
        
        if selected == "Carga Nota":
            upload_nf(session)

        elif selected == "Relatório":
            relatorio(session)
    
    else:
        with st.sidebar:
            # logo fgv
            selected = option_menu("Menu", ["Relatório"],
                                icons=['📈', '📈'], menu_icon="'📈'", default_index=0,
                                #    styles={
                                #     "nav-link-selected": {"background-color": "#105aff"},
                                #     }
            )

            col1, col2, col3 = st.columns(3)

            if col2.button('Sair'):
                time.sleep(1)
                st.session_state.clear()
                st.cache_data.clear()
                st.rerun()
        
        if selected == "Carga Nota":
            upload_nf(session)

        elif selected == "Relatório":
            relatorio(session)
        
