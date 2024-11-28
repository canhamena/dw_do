import requests
import pyodbc
from sqlalchemy import create_engine
import requests
import pandas as pd
import time
from datetime import datetime
# Configuração da conexão

server = r'NTBRCSDIT03\SPBDEV'  # Nome do servidor ou endereço IP
database = 'Comercio'  # Nome do banco de dados
driver = "ODBC Driver 17 for SQL Server"

# String de conexão com autenticação do Windows

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)



def carregamento():
    
    url = "https://api.exemplo.com/dados"

# Parâmetros da requisição (opcional)


# Cabeçalhos da requisição (opcional)
headers = {
    "Authorization": "Bearer seu_token_de_acesso",  # Se necessário
    "Content-Type": "application/json"
}
engine = create_engine(f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes")
conn = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;")

cursor = conn.cursor()

cursor.execute("Truncate TABLE requisicao_pessoal;")
cursor.commit()


cursor.close()
time.sleep(5) 
tabela_destino = "requisicao_pessoal"

link1 = "https://api.rcsangola.co.ao/api/requisicao-pessoal"

data_atual = datetime.now();

response = requests.get(link1, headers=headers)
data = response.json()
df = pd.DataFrame(data)
colunas_para_remover = ["cabinetId", "File type","Document size","Disk number","@CABINETNAME","Temporalidade"]
df = df.drop(columns=colunas_para_remover, errors="ignore")
try:
    df.to_sql(tabela_destino,engine, if_exists="replace", index=False)
    novo_cursor = conn.cursor()
    novo_cursor.execute("INSERT INTO controlo_insert (DataHora) VALUES (?)", (data_atual,))
    
    novo_cursor.commit()
    novo_cursor.close()
    print(f"Dados inseridos com sucesso na tabela {tabela_destino} e {"controlo_insert"} .")
except Exception as e:
    print(f"Erro ao inserir os dados: {e}")



# Visualizar os primeiros registros
"""try:
    # Fazendo uma requisição GET
    response = requests.get(link1, headers=headers)
    
    # Verificando se a requisição foi bem-sucedida
    if response.status_code == 200:
        # Convertendo a resposta para JSON
        data = response.json()
        print("Dados recebidos:")
        print(data[0]["Document ID"])
    else:
        print(f"Erro: {response.status_code}")
        print(response.text)

except requests.exceptions.RequestException as e:
    print(f"Erro na requisição: {e}")"""



