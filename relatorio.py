import streamlit as st
import pandas as pd
from connection_snowflake.connection import getSession, runQuery
import plotly.express as px

def main(session):
    session = getSession()
    if st.session_state.user_id != 201:
        query = f"SELECT * FROM DOC_AI_DB.FGV.NF_FGV where ID = {st.session_state.user_id} order by data_upload asc;"
    else:
        query = "SELECT * FROM DOC_AI_DB.FGV.NF_FGV order by data_upload asc;"
    with st.spinner('Carregando dados...'):
        data = runQuery(query, session)

    df = pd.DataFrame(data, columns=['ID', 
                                     'NOME',
                                     'ORDEM DE COMPRA', 
                                     'NOTA', 
                                     'DATA EMISSÃO', 
                                     'CNPJ PRESTADOR',
                                     'CNAE',
                                     'VALOR',
                                     'DISCIPLINA',
                                     'STATUS',
                                     'TURMA',
                                     'DATA UPLOAD',
                                     'DATA PAGAMENTO'])
    
    st.subheader('Histórico de Solicitações x Status')
    df_graph = df.groupby('STATUS').size().reset_index(name='QUANTIDADE')
    fig = px.bar(df_graph,
                x='STATUS',
                text_auto=True,
                y='QUANTIDADE',
                labels={'value': 'Quantidade', 'variable': 'Status'},
                color='STATUS',
                color_discrete_sequence=['#5ab2e3', '#2c5a71', '#011f4b']
                )
    fig.update_traces(textfont_size=18, textposition='outside', cliponaxis=False)
    st.plotly_chart(fig, use_container_width=True)

    min_date = df['DATA UPLOAD'].min()
    max_date = df['DATA UPLOAD'].max()

    st.subheader('Relatório de Notas')
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    try:
        with c1:
            min_date = st.date_input('Data início', min_date, format='DD/MM/YYYY')
        with c2:
            max_date = st.date_input('Data fim', max_date, format='DD/MM/YYYY')

        if min_date <= max_date:
            filtered_df = df[(df['DATA UPLOAD'] >= min_date) & (df['DATA UPLOAD'] <= max_date)]
            if st.session_state.user_id != 201:
                st.dataframe(filtered_df, column_order = ('NOME',
                                                            'ORDEM DE COMPRA', 
                                                            'NOTA', 
                                                            'DATA EMISSÃO', 
                                                            'DISCIPLINA',
                                                            'TURMA',
                                                            'VALOR', 
                                                            'STATUS', 
                                                            'DATA PAGAMENTO'),  hide_index=True, use_container_width=True)
            else:
                st.dataframe(filtered_df, column_order = ('NOME',
                                                            'ORDEM DE COMPRA', 
                                                            'NOTA', 
                                                            'DATA EMISSÃO', 
                                                            'CNPJ PRESTADOR',
                                                            'CNAE',
                                                            'DATA UPLOAD',
                                                            'VALOR', 
                                                            'STATUS', 
                                                            'DATA PAGAMENTO'),  hide_index=True, use_container_width=True)
        else:
            st.error('Error: A data início não pode ser maior que a data fim.')
    except Exception:
        st.error('Nenhuma nota carregada.')
