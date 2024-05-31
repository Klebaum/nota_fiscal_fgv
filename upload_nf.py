import streamlit as st
import pandas as pd
from snowflake.snowpark import FileOperation
from connection_snowflake.connection import  runQuery
import json
import os
from datetime import datetime, date

json_list = [
    {
    "CNAE": [
      {
        "score": 1,
        "value": "8533300"
      }
    ],
    "CNPJ_PRESTADOR": [
      {
        "score": 1,
        "value": "11.837.324/0001-03"
      }
    ],
    "CNPJ_TOMADOR": [
      {
        "score": 1,
        "value": "33.641.663/0001-44"
      }
    ],
    "DATA_EMISSAO": [
      {
        "score": 1,
        "value": "17/05/2024"
      }
    ],
    "DISCIPLINA": [
      {
        "score": 1,
        "value": "Aspectos Ambientais de Empreendimentos Imobiliários"
      }
    ],
    "NOTA": [
      {
        "score": 1,
        "value": "202400000000012"
      }
    ],
    "ORDEM_DE_COMPRA": [
      {
        "score": 1,
        "value": "54613"
      }
    ],
    "TURMA": [
      {
        "score": 1,
        "value": "ONLO23ZB-LGICILV2331"
      }
    ],
    "VALOR": [
      {
        "score": 1,
        "value": "6.360,00"
      }
    ],
    "__documentMetadata": {
      "ocrScore": 0.497
    }
    },

    {
        "CNAE": [
        {
            "score": 1,
            "value": "8599604"
        }
        ],
        "CNPJ_PRESTADOR": [
        {
            "score": 1,
            "value": "06.207.694/0001-19"
        }
        ],
        "CNPJ_TOMADOR": [
        {
            "score": 1,
            "value": "33.641.663/0012-05"
        }
        ],
        "DATA_EMISSAO": [
        {
            "score": 1,
            "value": "22/04/2024"
        }
        ],
        "DISCIPLINA": [
        {
            "score": 1,
            "value": "Governança corporativa e compliance"
        }
        ],
        "NOTA": [
        {
            "score": 1,
            "value": "126"
        }
        ],
        "ORDEM_DE_COMPRA": [
        {
            "score": 1,
            "value": "52596"
        }
        ],
        "TURMA": [
        {
            "score": 1,
            "value": "MBS02322-MDHG-T05"
        }
        ],
        "VALOR": [
        {
            "score": 1,
            "value": "6.360,00"
        }
        ],
        "__documentMetadata": {
        "ocrScore": 0.534
        }
    },

    {
    "CNAE": [
      {
        "score": 1,
        "value": "Não encontrado"
      }
    ],
    "CNPJ_PRESTADOR": [
      {
        "score": 1,
        "value": "24.507.193/0001-30"
      }
    ],
    "CNPJ_TOMADOR": [
      {
        "score": 1,
        "value": "33.641.663/0016-20"
      }
    ],
    "DATA_EMISSAO": [
      {
        "score": 1,
        "value": "15/05/2024"
      }
    ],
    "DISCIPLINA": [
      {
        "score": 1,
        "value": "Consumo e Processo Decisório da Compra"
      }
    ],
    "NOTA": [
      {
        "score": 1,
        "value": "209"
      }
    ],
    "ORDEM_DE_COMPRA": [
      {
        "score": 1,
        "value": "43054"
      }
    ],
    "TURMA": [
      {
        "score": 1,
        "value": "MSP02424-PGPMK-T6"
      }
    ],
    "VALOR": [
      {
        "score": 1,
        "value": "5.940,00"
      }
    ],
    "__documentMetadata": {
      "ocrScore": 0.545
    }
    }

]

@st.cache_data
def process_json_data(df):
    df_aux = pd.DataFrame()
    for json_data in json_list:
        fields_to_check = ["CNAE", "DATA_EMISSAO", "DISCIPLINA", "TURMA", "ORDEM_DE_COMPRA", "NOTA", "VALOR", "CNPJ_PRESTADOR"]
        for field in fields_to_check:
            if field in json_data and len(json_data[field]) > 0:
                if "value" not in json_data[field][0]:
                    json_data[field][0]["value"] = "Não encontrado"

        # st.write(json_data)

        df_aux = pd.DataFrame({
            'ID': st.session_state.user_id,
            'NOTA': [int(json_data['NOTA'][0]['value'])],
            'CNPJ PRESTADOR': [json_data['CNPJ_PRESTADOR'][0]['value']],
            'CNAE': [json_data['CNAE'][0]['value']],
            'DATA UPLOAD': date.today(),
            'DATA EMISSÃO': datetime.strptime(json_data['DATA_EMISSAO'][0]['value'], "%d/%m/%Y").date(),
            'VALOR': [float(json_data['VALOR'][0]['value'].replace('R$', '').replace('.', '').replace(',', '.'))],
            'ORDEM DE COMPRA': [int(json_data['ORDEM_DE_COMPRA'][0]['value'])],
            'DISCIPLINA': json_data['DISCIPLINA'][0]['value'],
            'TURMA': json_data['TURMA'][0]['value'].replace(' ', ''),   
            'DATA PAGAMENTO': date.today(),
            'STATUS': 'NF Pendente'
        })
    
        # Concatena o DataFrame auxiliar ao DataFrame principal
        df = pd.concat([df, df_aux], ignore_index=True)
    return df

@st.cache_data
def load_data(_session, uploaded_file, df):
    df_aux = pd.DataFrame()
    try:
        for i in range(len(uploaded_file)):
            FileOperation(_session).put_stream(input_stream=uploaded_file[i], stage_location='@'+'doc_ai_stage'+'/'+uploaded_file[i].name, auto_compress=False)
            query = f"SELECT DOC_AI_DB.FGV.NF_FGV!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{uploaded_file[i].name}'), 6);"
            data = runQuery(query, _session)
            data = data[0][0]

            json_data = json.loads(data)
            
            if "CNAE" in json_data and len(json_data["CNAE"]) > 0:
                if "value" not in json_data["CNAE"][0]:
                    json_data["CNAE"][0]["value"] = "Não encontrado"

            if "DATA_EMISSAO" in json_data and len(json_data["DATA_EMISSAO"]) > 0:
                if "value" not in json_data["DATA_EMISSAO"][0]:
                    json_data["DATA_EMISSAO"][0]["value"] = "Não encontrado"
            
            if "DISCIPLINA" in json_data and len(json_data["DISCIPLINA"]) > 0:
                if "value" not in json_data["DISCIPLINA"][0]:
                    json_data["DISCIPLINA"][0]["value"] = "Não encontrado"
            
            if "TURMA" in json_data and len(json_data["TURMA"]) > 0:
                if "value" not in json_data["TURMA"][0]:
                    json_data["TURMA"][0]["value"] = "Não encontrado"
            
            if "ORDEM_DE_COMPRA" in json_data and len(json_data["ORDEM_DE_COMPRA"]) > 0:
                if "value" not in json_data["ORDEM_DE_COMPRA"][0]:
                    json_data["ORDEM_DE_COMPRA"][0]["value"] = "Não encontrado"

            if "NOTA" in json_data and len(json_data["NOTA"]) > 0:
                if "value" not in json_data["NOTA"][0]:
                    json_data["NOTA"][0]["value"] = "Não encontrado"
            
            if "VALOR" in json_data and len(json_data["VALOR"]) > 0:
                if "value" not in json_data["VALOR"][0]:
                    json_data["VALOR"][0]["value"] = "Não encontrado"
            
            if "CNPJ_PRESTADOR" in json_data and len(json_data["CNPJ_PRESTADOR"]) > 0:
                if "value" not in json_data["CNPJ_PRESTADOR"][0]:
                    json_data["CNPJ_PRESTADOR"][0]["value"] = "Não encontrado"

            # st.write(json_data)

            df_aux = pd.DataFrame({
                'ID': st.session_state.user_id,
                'NOME': 'Guilhermo Lopes', # 'NOME': [json_data['NOME'][0]['value']],
                'NOTA': [int(json_data['NOTA'][0]['value'])],
                'CNPJ PRESTADOR': [json_data['CNPJ_PRESTADOR'][0]['value']],
                'CNAE': [json_data['CNAE'][0]['value']],
                'DATA UPLOAD' : date.today(),
                'DATA EMISSÃO': datetime.strptime(json_data['DATA_EMISSAO'][0]['value'], "%d/%m/%Y").date(),
                'VALOR': [float(json_data['VALOR'][0]['value'].replace('R$', '').replace('.', '').replace(',', '.'))],
                'ORDEM DE COMPRA': [int(json_data['ORDEM_DE_COMPRA'][0]['value'])],
                'DISCIPLINA': json_data['DISCIPLINA'][0]['value'],
                'TURMA': json_data['TURMA'][0]['value'].replace(' ', ''),   
                'DATA PAGAMENTO' : date.today(),
                'STATUS': 'NF Pendente'
            })
            df = pd.concat([df, df_aux], ignore_index=True)

        return df
    except Exception as e:
        st.write(e)

def nf_errors_field(df_atual, df_esperado):
    erros = {}
    cols = df_atual.columns

    for i in range(len(df_atual)):
        erros_linha = []
        
        for col in cols:
            if col != 'DATA UPLOAD' and col != 'DATA PAGAMENTO' and col != 'CNAE' and col != 'NOME':
                if col in df_esperado.columns:
                    valor_atual = df_atual.iloc[i][col]
                    valor_esperado = df_esperado.iloc[i][col]
                    if valor_atual != valor_esperado:
                        erros_linha.append({
                            'nota': df_atual.iloc[i]['NOTA'],
                            'campo': col,
                            'valor_atual': valor_atual,
                            'valor_esperado': valor_esperado
                        })
                else:
                    raise st.error(f"colunas {col} não encontrada no dataframe esperado.")

        if erros_linha:
            erros[f"{i}"] = erros_linha
    
    return erros

def clean_uploader():
    st.session_state.uploaded_file = []


def main(session):
    st.image('https://portal.fgv.br/sites/portal.fgv.br/themes/portalfgv/logo.png')
    df = pd.DataFrame()
    st.session_state.uploaded_file = st.file_uploader("Escolha um ou mais arquivos", type=['pdf'], accept_multiple_files=True, key='file_uploader', help='Upload de arquivos PDFs.')
    uploaded_file = st.session_state.uploaded_file
    # st.write(uploaded_file)
    if uploaded_file is not None:
        df = load_data(session, uploaded_file, df)
    # df = process_json_data(df)
    
    #Retirar CNAE
    with st.container():
        st.subheader('Dados das Notas Fiscais')
        if df.empty:
            st.error('Nenhuma nota carregada')
            df_empty = pd.DataFrame({
                'ORDEM DE COMPRA': [' '] * 5,
                'NOTA': [' '] * 5,
                'DATA EMISSÃO': [' '] * 5,
                'CNPJ PRESTADOR': [' '] * 5,
                'VALOR': [' '] * 5,
                'DISCIPLINA': [' '] * 5,
                'STATUS': [' '] * 5,
                'TURMA': [' '] * 5,
                'DATA UPLOAD': [' '] * 5,
                'DATA PAGAMENTO': [' '] * 5
            })
            st.dataframe(df_empty, width=1000, hide_index=True, use_container_width=True)


        ### LEMBRAR DE TIRAR O O CJPJ DO PRESTADOR E COLOCAR PRESTADOR
        else:
            edited_df = st.data_editor(df, key="key", hide_index=True, disabled='STATUS', width=1000, 
                                    column_order=(  
                                                    'ORDEM DE COMPRA', 
                                                    'NOTA', 
                                                    'DATA EMISSÃO', 
                                                    'CNPJ PRESTADOR', 
                                                    'VALOR', 
                                                    'DISCIPLINA',
                                                    'TURMA',
                                                    'STATUS', 
                                                    'DATA PAGAMENTO'), use_container_width=True)
            df_to_snowflake = pd.DataFrame(edited_df)       
            # st.write(df) 

            erros = {} 
            nf_data = runQuery('select * from nf_confirmacao;', session)
            df_nf_confirmacao = pd.DataFrame(nf_data, columns=[
                                    'ID', 
                                    'ORDEM DE COMPRA', 
                                    'NOTA', 
                                    'DATA EMISSÃO', 
                                    'CNPJ PRESTADOR',
                                    'CNAE',
                                    'VALOR',
                                    'DISCIPLINA',
                                    'STATUS',
                                    'TURMA',]
                                )
    
            erros = nf_errors_field(df_to_snowflake, df_nf_confirmacao)
            # st.write(erros)

            if erros:
                st.subheader("Erros encontrados:")
                st.write(f"\nNota: 209")

                for nota, lista_erros in erros.items():
                    # st.write(f"\nNota {int(nota)+1}:")
                    # Itera sobre os erros em cada linha
                    for erro in lista_erros:
                        a = erro['nota']
                        campo = erro['campo']
                        valor_atual = erro['valor_atual']
                        valor_esperado = erro['valor_esperado']
                        st.error(f"O campo '{campo}' está diferente: \n Valor atual: {valor_atual} \n Valor esperado: {valor_esperado}")
                    erros_df = pd.DataFrame(lista_erros)
                    erros_df = erros_df.rename(columns={'campo': 'CAMPO', 'valor_atual': 'VALOR ATUAL', 'valor_esperado': 'VALOR ESPERADO'})
                    # st.dataframe(erros_df, hide_index=True)
                # st.error(f"Dado do campo {erros_df['CAMPO'][0]} incorreto.\nDado atual: {erros_df['VALOR ATUAL'][0]}\nDado desejado:{erros_df['VALOR ESPERADO'][0]}\nCorrija o campo antes de enviar as notas.")
            
            else:
                st.success(f"Notas prontas para envio!")
        try:
            if st.button("Enviar Notas", disabled=len(erros)!=0, on_click=clean_uploader):
                if erros:
                    st.write('Corrigir campos incorretos antes de enviar as notas.')
                else:
                    for i in range(len(df_to_snowflake)):
                        try:    
                            query = f"""INSERT INTO DOC_AI_DB.FGV.NF_FGV VALUES (
                                {df_to_snowflake['ID'][i]},
                                '{df_to_snowflake['NOME'][i]}',
                                {df_to_snowflake['ORDEM DE COMPRA'][i]}, 
                                {df_to_snowflake['NOTA'][i]}, 
                                '{df_to_snowflake['DATA EMISSÃO'][i]}', 
                                '{df_to_snowflake['CNPJ PRESTADOR'][i]}', 
                                '{df_to_snowflake['CNAE'][i]}',
                                {df_to_snowflake['VALOR'][i]},
                                '{df_to_snowflake['DISCIPLINA'][i]}', 
                                '{df_to_snowflake['STATUS'][i]}',
                                '{df_to_snowflake['TURMA'][i]}',
                                '{df_to_snowflake['DATA UPLOAD'][i]}', 
                                '{df_to_snowflake['DATA PAGAMENTO'][i]}');"""
                            runQuery(query, session)
                        except Exception as e:
                            st.error(f'Erro ao enviar notas: {e}')
                    st.success('Notas enviadas com sucesso!')
                
                # st.cache_data.clear()
                # st.rerun()
        except Exception:
            st.button("Enviar Notas", disabled=True)
                