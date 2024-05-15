import streamlit as st
st.set_page_config(layout="wide")
import pandas as pd
from snowflake.snowpark import FileOperation
from connection_snowflake.connection import getSession, runQuery
import json

session = getSession()

df = pd.DataFrame()

uploaded_file = st.file_uploader("Escolha um ou mais arquivos", type=['pdf', 'zip'], accept_multiple_files=True, key='file_uploader', help='Upload de arquivos PDF ou ZIP contendo PDFs.')

# css = '''
# <style>
#     [data-testid='stFileUploader'] {
#         width: max-content;
#     }
#     [data-testid='stFileUploader'] section {
#         padding: 0;
#         float: left;
#     }
#     [data-testid='stFileUploader'] section > input + div {
#         display: none;
#     }
#     [data-testid='stFileUploader'] section + div {
#         float: right;
#         padding-top: 0;
#     }

# </style>
# '''

# st.markdown(css, unsafe_allow_html=True)

if uploaded_file is not None:
    try:
        for i in range(len(uploaded_file)):
            FileOperation(session).put_stream(input_stream=uploaded_file[i], stage_location='@'+'doc_ai_stage'+'/'+uploaded_file[i].name, auto_compress=False)
            query = f"SELECT DOC_AI_DB.FGV.NF_FGV!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{uploaded_file[i].name}'), 1);"
            data = runQuery(query, session)
            data = data[0][0]

            json_data = json.loads(data)
            # st.write(json_data)

            df_aux = pd.DataFrame({
                'CNAE_score': [json_data['CNAE'][0]['score']],
                'NOTA_score': [json_data['NOTA'][0]['score']],
                'NOTA_value': [json_data['NOTA'][0]['value']],
                'VALOR_score': [json_data['VALOR'][0]['score']],
                'VALOR_value': [json_data['VALOR'][0]['value']],
                'ocrScore': [json_data['__documentMetadata']['ocrScore']],
                'Status': 'pendente'
            })

            df = pd.concat([df, df_aux], ignore_index=True)
    except Exception as e:
        st.write(e)

with st.container():
    st.subheader('Notas Carregadas')
    if df.empty:
        st.write('Nenhuma nota carregada')
        df_empty = pd.DataFrame({
            'NOTA': [''] * 5,
            'CNAE': [''] * 5,
            'VALOR': [''] * 5,
            'STATUS': [''] * 5
        })
        st.dataframe(df_empty, width=1000, height=200)
    else:
        edited_df = st.data_editor(df, hide_index=True, disabled='Status')
    if st.button("Enviar Notas"):
        st.write("Notas enviadas com sucesso!")

