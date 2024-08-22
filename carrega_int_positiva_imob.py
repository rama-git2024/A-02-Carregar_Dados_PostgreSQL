pip install pandas psycopg2 sqlalchemy docker

import pandas as pd
import docker
import tarfile
from io import BytesIO
from sqlalchemy import create_engine

# Configurações do banco de dados
db_config = {
    'dbname': 'rama',
    'user': 'pcecere',
    'password': '244049',
    'host': 'localhost',
    'port': 5434
}

# Caminho para o arquivo Excel e CSV
excel_file_path = r'X:\Recuperação\Crédito Imobiliário\Acompanhamento\Controle de Intimações Positivas 2024.xlsx'
csv_file_path = r'C:\Users\pedro.cecere\Desktop\dados.csv'
sheet_name = 'Dados'  # Altere para o nome da aba correta


def read_and_process_excel(excel_file_path, sheet_name):

	# Ler a planilha do Excel
	df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

	# Alterar o nome das colunas do df
	rename_df = {
	    'Dt. Distr. (AF 1.1)': 'af_11',
	    'Dt. Intim. Todos (AF 2.8)': 'af_28',
	    'Dt. Ciência Intim.': 'todos_citados',
	    'TM Intim. (dias)': 'tm_intimacao',
	    'Tipo Intim.': 'tipo_intimacao',
	    'Produto': 'produto',
	    'Dossiê nº': 'dossie',
	    'Adverso': 'adverso',
	    'Comarca': 'comarca',
	    'UF': 'uf',
	    'Últ. Evento WF': 'ultimo_evento_wf',
	    'Responsável Intimação': 'resp_intmacao',
	    'Responsável Consolidação': 'resp_consolidacao',
	    'Mês': 'mes'
	}
	df.rename(columns=rename_df, inplace=True)

	# Alterar tipo de dado das colunas para tipo date
	date_columns = ['af_11', 'af_28', 'todos_citados']
	for col in date_columns:
	    df[col] = pd.to_datetime(df[col]).dt.date

	return df

def save_df_to_csv(df, csv_file_path):

	# Salvar o DataFrame em um arquivo CSV com encoding UTF-8-SIG
	df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')

def copy_csv_to_docker_container(csv_file_path, container_id):

	# Configurações do Docker
	container_id = '223c59c1268f'  # Substitua pelo ID do seu container

	# Copiar o arquivo CSV para o container Docker
	client = docker.from_env()
	container = client.containers.get(container_id)

	# Criar um arquivo tar do CSV
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
    truncate_command = f"psql -U {db_config['user']} -d {db_config['dbname']} -c \"TRUNCATE TABLE imobiliario.int_positiva\""
    load_csv_command = f"psql -U {db_config['user']} -d {db_config['dbname']} -c \"COPY imobiliario.int_positiva FROM '/tmp/{csv_file_name}' DELIMITER ',' CSV HEADER\""

    execute_command_in_container(container_id, truncate_command)
    execute_command_in_container(container_id, load_csv_command)

if __name__ == "__main__":
    df = read_and_process_excel(excel_file_path, sheet_name)
    save_df_to_csv(df, csv_file_path)
    copy_csv_to_docker_container(csv_file_path, container_id)
    truncate_and_load_data(db_config, container_id, 'dados.csv')
    os.remove(csv_file_path)

    print("Dados carregados com sucesso!")