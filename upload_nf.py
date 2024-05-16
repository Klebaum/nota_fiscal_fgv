import streamlit as st
import pandas as pd
from snowflake.snowpark import FileOperation
from connection_snowflake.connection import  runQuery
import json
from datetime import datetime



@st.cache_data
def load_data(_session, uploaded_file, df):
    df_aux = pd.DataFrame()
    try:
        for i in range(len(uploaded_file)):
            FileOperation(_session).put_stream(input_stream=uploaded_file[i], stage_location='@'+'doc_ai_stage'+'/'+uploaded_file[i].name, auto_compress=False)
            query = f"SELECT DOC_AI_DB.FGV.NF_FGV!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{uploaded_file[i].name}'), 1);"
            data = runQuery(query, _session)
            data = data[0][0]

            json_data = json.loads(data)

            if "CNAE" in json_data and len(json_data["CNAE"]) > 0:
                if "value" not in json_data["CNAE"][0]:
                    json_data["CNAE"][0]["value"] = "Não encontrado"

            if "NOTA" in json_data and len(json_data["NOTA"]) > 0:
                if "value" not in json_data["NOTA"][0]:
                    json_data["NOTA"][0]["value"] = "Não encontrado"
            
            if "VALOR" in json_data and len(json_data["VALOR"]) > 0:
                if "value" not in json_data["VALOR"][0]:
                    json_data["VALOR"][0]["value"] = "Não encontrado"

            df_aux = pd.DataFrame({
                #'CNAE_score': [json_data['CNAE'][0]['score']],
                'ID': st.session_state.user_id,
                'NOTA': [json_data['NOTA'][0]['value']],
                'CNAE': [json_data['CNAE'][0]['value']],
                'Data Upload' : datetime.now(), #.strftime("%d/%m/%Y %H:%M:%S")
                #'NOTA_score': [json_data['NOTA'][0]['score']],
                #'VALOR_score': [json_data['VALOR'][0]['score']],
                'VALOR': [float(json_data['VALOR'][0]['value'].replace('R$', '').replace('.', '').replace(',', '.'))],
                #'ocrScore': [json_data['__documentMetadata']['ocrScore']],
                'Ordem de Compra': '',
                'Status': 'pendente'
            })
            df = pd.concat([df, df_aux], ignore_index=True)
        return df
    except Exception as e:
        st.write(e)

def main(session):
    df = pd.DataFrame()
    uploaded_file = st.file_uploader("Escolha um ou mais arquivos", type=['pdf', 'zip'], accept_multiple_files=True, key='file_uploader', help='Upload de arquivos PDF ou ZIP contendo PDFs.')
    if uploaded_file is not None:
        df = load_data(session, uploaded_file, df)

    with st.container():
        st.subheader('Notas Carregadas')
        if df.empty:
            st.error('Nenhuma nota carregada')
            df_empty = pd.DataFrame({
                'NOTA': [''] * 5,
                'CNAE': [''] * 5,
                'VALOR': [''] * 5,
                'ORDEM DE COMPRA': [''] * 5,
                'STATUS': [''] * 5
            })
            st.dataframe(df_empty, width=1000, hide_index=True)

        else:
            edited_df = st.data_editor(df, key="key", hide_index=True, disabled='Status', width=1000, column_order=('NOTA', 'CNAE', 'Data Upload', 'VALOR', 'Ordem de Compra', 'Status'))
            df_to_snowflake = pd.DataFrame(edited_df)        

        if st.button("Enviar Notas"):
            for i in range(len(df_to_snowflake)):
                query = f"INSERT INTO DOC_AI_DB.FGV.NF_FGV VALUES ({df_to_snowflake['ID'][i]}, {df_to_snowflake['NOTA'][i]}, '{df_to_snowflake['CNAE'][i]}', '{df_to_snowflake['Data Upload'][i]}', {df_to_snowflake['VALOR'][i]}, '{df_to_snowflake['Ordem de Compra'][i]}', '{df_to_snowflake['Status'][i]}');"
                runQuery(query, session)
            st.success("Notas enviadas com sucesso!")