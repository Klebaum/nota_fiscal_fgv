import streamlit as st
from connection_snowflake.connection import runQuery
import pandas as pd
import time
 
def login(session):
    with st.form(key='login'):
        st.title('Login')
        username = st.text_input('Usuário')
        password = st.text_input('Senha', type='password')
        
        submit_button = st.form_submit_button('Entrar')

        if submit_button:
            query = f"select * from login_fgv where user = '{username}' and password = '{password}';"
            data = runQuery(query, session)
            data = pd.DataFrame(data)
            if username == data['USER'][0] and password == data['PASSWORD'][0]:
                st.success('Logado com sucesso')
                st.session_state['connection_established'] = True
                st.session_state['user_id'] = data['ID'][0]
                time.sleep(2)
                st.rerun()
            else:
                st.error('Usuário ou senha incorretos')
    if 'button_pressed' not in st.session_state:
        st.session_state['button_pressed'] = False