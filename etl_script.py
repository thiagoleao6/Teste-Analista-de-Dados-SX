import pandas as pd
import mysql.connector
import os

# Configurações do banco de dados
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'mysql'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'password'),
    'database': os.getenv('MYSQL_DATABASE', 'etl_db')
}

CSV_FILE = r"C:\Users\thiag\Downloads\MICRODADOS_ENEM_2020.csv"

def load_csv_to_mysql():
    # Ler o arquivo CSV
    df = pd.read_csv(CSV_FILE)
    notas = df[["NU_INSCRICAO", "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", 
                "NU_NOTA_MT", "NU_NOTA_REDACAO", "TP_PRESENCA_CN", "TP_PRESENCA_LC"]]
    
    # Conectar ao banco de dados MySQL
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Criar tabela 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS notas(
            id INT AUTO_INCREMENT PRIMARY KEY,
            NU_INSCRICAO INT,
            NU_NOTA_CN INT,
            NU_NOTA_CH INT,
            NU_NOTA_LC INT,
            NU_NOTA_MT INT,
            NU_NOTA_REDACAO INT,
            TP_PRESENCA_CN INT,
            TP_PRESENCA_LC INT
        )
    """)

    # Inserir dados
    for _, row in notas.iterrows():
        cursor.execute("""
            INSERT INTO notas (NU_INSCRICAO, NU_NOTA_CN, NU_NOTA_CH, NU_NOTA_LC, 
                NU_NOTA_MT, NU_NOTA_REDACAO, TP_PRESENCA_CN, TP_PRESENCA_LC)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['NU_INSCRICAO'], row['NU_NOTA_CN'], row['NU_NOTA_CH'], 
              row['NU_NOTA_LC'], row['NU_NOTA_MT'], row['NU_NOTA_REDACAO'], 
              row['TP_PRESENCA_CN'], row['TP_PRESENCA_LC']))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    load_csv_to_mysql()

#___________________________________________________2° tabela ________________________________________________________________
    

def load_csv_to_mysql2():
    # Ler o arquivo CSV
    df = pd.read_csv(CSV_FILE)
    info_aluno = df[["NU_INSCRICAO", "TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA", 
                     "TP_ST_CONCLUSAO", "TP_ESCOLA", "IN_TREINEIRO", "CO_MUNICIPIO_ESC", 
                     "NO_MUNICIPIO_ESC", "SG_UF_ESC", "TP_DEPENDENCIA_ADM_ESC", "TP_LOCALIZACAO_ESC"]]
    
    # Conectar ao banco de dados MySQL
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Criar tabela para informações do aluno
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS info_aluno (
            id INT AUTO_INCREMENT PRIMARY KEY,
            NU_INSCRICAO INT,
            TP_FAIXA_ETARIA INT,
            TP_SEXO VARCHAR(1),
            TP_COR_RACA INT,
            TP_ST_CONCLUSAO INT,
            TP_ESCOLA INT,
            IN_TREINEIRO INT,
            CO_MUNICIPIO_ESC INT,
            NO_MUNICIPIO_ESC VARCHAR(30),
            SG_UF_ESC VARCHAR(30),
            TP_DEPENDENCIA_ADM_ESC INT,
            TP_LOCALIZACAO_ESC INT
        )
    """)

    # Inserir dados na tabela info_aluno
    for _, row in info_aluno.iterrows():
        cursor.execute("""
            INSERT INTO info_aluno (NU_INSCRICAO, TP_FAIXA_ETARIA, TP_SEXO, TP_COR_RACA, 
                TP_ST_CONCLUSAO, TP_ESCOLA, IN_TREINEIRO, CO_MUNICIPIO_ESC, 
                NO_MUNICIPIO_ESC, SG_UF_ESC, TP_DEPENDENCIA_ADM_ESC, TP_LOCALIZACAO_ESC)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['NU_INSCRICAO'], row['TP_FAIXA_ETARIA'], row['TP_SEXO'], row['TP_COR_RACA'], 
              row['TP_ST_CONCLUSAO'], row['TP_ESCOLA'], row['IN_TREINEIRO'], row['CO_MUNICIPIO_ESC'], 
              row['NO_MUNICIPIO_ESC'], row['SG_UF_ESC'], row['TP_DEPENDENCIA_ADM_ESC'], row['TP_LOCALIZACAO_ESC']))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    load_csv_to_mysql2()



#___________________________________________________3° tabela ________________________________________________________________
    

def load_csv_to_mysql3():
    # Ler o arquivo CSV
    df = pd.read_csv(CSV_FILE)
    
    # Selecionar as colunas relevantes para a nova tabela
    info_renda = df[["NU_INSCRICAO", "Q006", "Q008", "Q024", "Q025"]]
    
    # Conectar ao banco de dados MySQL
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Criar tabela para informações de renda
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS info_renda (
            id INT AUTO_INCREMENT PRIMARY KEY,
            NU_INSCRICAO INT,
            Q006 VARCHAR(255),  
            Q008 INT,
            Q024 VARCHAR(255), 
            Q025 INT
        )
    """)

    # Inserir dados na tabela info_renda usando executemany para otimização
    insert_query = """
        INSERT INTO info_renda (NU_INSCRICAO, Q006, Q008, Q024, Q025)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    # Prepare os dados para inserção
    data_to_insert = [(row['NU_INSCRICAO'], row['Q006'], row['Q008'], row['Q024'], row['Q025']) 
                      for _, row in info_renda.iterrows()]
    
    # Inserir várias linhas ao mesmo tempo para otimizar a performance
    cursor.executemany(insert_query, data_to_insert)

    # Confirmar a transação
    conn.commit()

    # Fechar a conexão com o banco de dados
    cursor.close()
    conn.close()

if __name__ == '__main__':
    load_csv_to_mysql3()


