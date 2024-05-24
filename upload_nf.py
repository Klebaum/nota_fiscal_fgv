import streamlit as st
import pandas as pd
from snowflake.snowpark import FileOperation
from connection_snowflake.connection import  runQuery
import json
from datetime import date

@st.cache_data
def load_data(_session, uploaded_file, df):
    df_aux = pd.DataFrame()
    try:
        for i in range(len(uploaded_file)):
            FileOperation(_session).put_stream(input_stream=uploaded_file[i], stage_location='@'+'doc_ai_stage'+'/'+uploaded_file[i].name, auto_compress=False)
            query = f"SELECT DOC_AI_DB.FGV.NF_FGV!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{uploaded_file[i].name}'), 5);"
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
            
            if "CNPJ_TOMADOR" in json_data and len(json_data["CNPJ_TOMADOR"]) > 0:
                if "value" not in json_data["CNPJ_TOMADOR"][0]:
                    json_data["CNPJ_TOMADOR"][0]["value"] = "Não encontrado"

            df_aux = pd.DataFrame({
                'ID': st.session_state.user_id,
                'NOTA': [json_data['NOTA'][0]['value']],
                'CNPJ TOMADOR': [json_data['CNPJ_TOMADOR'][0]['value']],
                'CNAE': [json_data['CNAE'][0]['value']],
                'DATA DE UPLOAD' : date.today(),
                'DATA DE EMISSÃO': date.today(),
                'VALOR': [float(json_data['VALOR'][0]['value'].replace('R$', '').replace('.', '').replace(',', '.'))],
                'ORDEM DE COMPRA': 6464545,
                'DISCIPLINA': 'abc, cdf',
                'TURMA': 'abc',    
                'DATA DE PAGAMENTO': date.today(),
                'STATUS': 'NF pendente'
            })
            df = pd.concat([df, df_aux], ignore_index=True)
        return df
    except Exception as e:
        st.write(e)

def main(session):
    df = pd.DataFrame()
    uploaded_file = st.file_uploader("Escolha um ou mais arquivos", type=['pdf'], accept_multiple_files=True, key='file_uploader', help='Upload de arquivos PDF ou ZIP contendo PDFs.')
    if uploaded_file is not None:
        df = load_data(session, uploaded_file, df)

    with st.container():
        st.subheader('Dados das Notas Fiscais')
        if df.empty:
            st.error('Nenhuma nota carregada')
            df_empty = pd.DataFrame({
                'ORDEM DE COMPRA': [' '] * 5,
                'NOTA': [' '] * 5,
                'DATA EMISSÃO': [' '] * 5,
                'CNPJ TOMADOR': [' '] * 5,
                'CNAE': [' '] * 5,
                'VALO': [' '] * 5,
                'DISCIPLINA': [' '] * 5,
                'STATUS': [' '] * 5,
                'TURMA': [' '] * 5,
                'DATA DE UPLOAD': [' '] * 5,
                'DATA DE PAGAMENTO': [' '] * 5
            })
            st.dataframe(df_empty, width=1000, hide_index=True, use_container_width=True)

        else:
            edited_df = st.data_editor(df, key="key", hide_index=True, disabled='STATUS', width=1000, column_order=('ORDEM DE COMPRA', 'NOTA', 'DATA DE EMISSÃO', 'CNPJ TOMADOR', 'VALOR', 'DISCIPLINA', 'DATA DE PAGAMENTO'), use_container_width=True)
            df_to_snowflake = pd.DataFrame(edited_df)        

        try:
            if st.button("Enviar Notas"):
                for i in range(len(df_to_snowflake)):
                    query = f"""INSERT INTO DOC_AI_DB.FGV.NF_FGV VALUES (
                        {df_to_snowflake['ID'][i]},
                        {df_to_snowflake['ORDEM DE COMPRA'][i]}, 
                        {df_to_snowflake['NOTA'][i]}, 
                        '{df_to_snowflake['DATA DE EMISSÃO'][i]}', 
                        '{df_to_snowflake['CNPJ TOMADOR'][i]}', 
                        '{df_to_snowflake['CNAE'][i]}',
                        {df_to_snowflake['VALOR'][i]},
                        '{df_to_snowflake['DISCIPLINA'][i]}', 
                        '{df_to_snowflake['STATUS'][i]}',
                        '{df_to_snowflake['TURMA'][i]}',
                        '{df_to_snowflake['DATA DE UPLOAD'][i]}'), 
                        '{df_to_snowflake['DATA DE PAGAMENTO'][i]}');"""
                    runQuery(query, session)
                st.success("Notas enviadas com sucesso!")
        except Exception as e:
            st.error('Erro ao enviar notas, campos obrigatórios não preenchidos.')
            st.error(e)