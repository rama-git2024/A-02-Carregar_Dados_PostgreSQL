
import pandas as pd
import docker
import tarfile
from io import BytesIO
from sqlalchemy import create_engine
import os

# Configurações do banco de dados
db_config = {
    'dbname': 'rama',
    'user': 'pcecere',
    'password': '244049',
    'host': 'localhost',
    'port': 5434
}

# Caminho para o arquivo Excel e CSV
excel_file_path = r'X:\Recuperação\Crédito Imobiliário\Acompanhamento\Distribuições ONR – 2024 v1.xlsx'
csv_file_path = r'C:\Users\pedro.cecere\Desktop\dados.csv'
sheet_name = 'Dados'  # Altere para o nome da aba correta
container_id = '223c59c1268f'  # Substitua pelo ID do seu container

def read_and_process_excel(excel_file_path, sheet_name):
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

    rename_df = {
        'Data AF 0.7': 'af_07',
        'Data AF 1.1': 'af_11',
        'Data Distribuição': 'data_distribuicao',
        'Código': 'codigo',
        'Dossiê': 'dossie',
        'Nome do Mutuário': 'adverso',
        'Produto': 'produto',
        'Responsável': 'responsavel',
        'Mês Distr.': 'mes_distribuicao',
        'Temp. Distr.': 'tempo_distribuicao'
    }
    df.rename(columns=rename_df, inplace=True)

    df = df.dropna(subset=['tempo_distribuicao'])

    date_columns = ['af_07', 'af_11', 'data_distribuicao']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col]).dt.date

    df['tempo_distribuicao'] = df['tempo_distribuicao'].astype(int)

    return df

def save_df_to_csv(df, csv_file_path):
    df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')

def copy_csv_to_docker_container(csv_file_path, container_id):
    client = docker.from_env()
    container = client.containers.get(container_id)

    with BytesIO() as tar_stream:
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tar.add(csv_file_path, arcname='dados.csv')
        tar_stream.seek(0)
        container.put_archive('/tmp', tar_stream)

def execute_command_in_container(container_id, command):
    client = docker.from_env()
    container = client.containers.get(container_id)
    exec_id = container.exec_run(command)
    print(exec_id.output.decode())

def truncate_and_load_data(db_config, container_id, csv_file_name):
    truncate_command = f"psql -U {db_config['user']} -d {db_config['dbname']} -c \"TRUNCATE TABLE imobiliario.distribuicao\""
    load_csv_command = f"psql -U {db_config['user']} -d {db_config['dbname']} -c \"COPY imobiliario.distribuicao FROM '/tmp/{csv_file_name}' DELIMITER ',' CSV HEADER\""

    execute_command_in_container(container_id, truncate_command)
    execute_command_in_container(container_id, load_csv_command)

if __name__ == "__main__":
    df = read_and_process_excel(excel_file_path, sheet_name)
    save_df_to_csv(df, csv_file_path)
    copy_csv_to_docker_container(csv_file_path, container_id)
    truncate_and_load_data(db_config, container_id, 'dados.csv')
    os.remove(csv_file_path)

    print("Dados carregados com sucesso!")
