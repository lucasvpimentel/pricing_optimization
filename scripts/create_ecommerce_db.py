# scripts/create_ecommerce_db.py

import pandas as pd
import sqlite3
from pathlib import Path

# --- Configurações ---
# Define o diretório base do projeto (um nível acima de 'scripts')
BASE_DIR = Path(__file__).resolve().parent.parent

# Caminhos dos arquivos usando pathlib
CSV_FILE_PATH = BASE_DIR / 'data' / 'ecommerce-data.csv'
DB_FILE_PATH = BASE_DIR / 'data' / 'ecommerce.db'
TABLE_NAME = 'transactions'

# Lista das colunas que você deseja usar
COLUMNS_TO_USE = [
    'InvoiceNo',
    'StockCode',
    'Description',
    'Quantity',
    'InvoiceDate',
    'UnitPrice',
    'CustomerID',
    'Country'
]

def create_db_from_csv(csv_path: Path, db_path: Path, table_name: str, columns_to_use: list):
    """
    Carrega dados de um CSV, seleciona colunas específicas, realiza limpeza básica
    e os salva em um banco de dados SQLite.
    """
    print(f"Iniciando a criação do banco de dados '{db_path}' a partir de '{csv_path}'...\n")

    if not csv_path.exists():
        print(f"Erro: O arquivo CSV '{csv_path}' não foi encontrado. Por favor, verifique o caminho.\n")
        return

    # Garante que o diretório 'data' exista
    db_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Carregar o dataset completo (para garantir que todas as colunas existem antes de selecionar)
        df = pd.read_csv(csv_path, encoding='ISO-8859-1')
        print(f"CSV '{csv_path}' carregado com sucesso. Total de {len(df)} linhas.\n")

        # 2. Selecionar apenas as colunas desejadas
        missing_cols = [col for col in columns_to_use if col not in df.columns]
        if missing_cols:
            print(f"Aviso: As seguintes colunas não foram encontradas no CSV e serão ignoradas: {missing_cols}\n")
            columns_to_use = [col for col in columns_to_use if col in df.columns]

        df = df[columns_to_use]
        print(f"Colunas selecionadas: {columns_to_use}\n")

        # 3. Limpeza e Pré-processamento Básicos
        print("Realizando limpeza e pré-processamento básicos...")

        # Converte 'InvoiceDate' para datetime
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

        # Remove linhas com valores nulos críticos para análise
        df.dropna(subset=['StockCode', 'Quantity', 'UnitPrice', 'InvoiceDate', 'InvoiceNo'], inplace=True)

        # Remove transações com quantidade ou preço unitário <= 0 (devoluções ou dados inválidos)
        df = df[df['Quantity'] > 0]
        df = df[df['UnitPrice'] > 0]

        if 'Description' in df.columns:
            df['Description'] = df['Description'].astype(str).str.strip()

        df['StockCode'] = df['StockCode'].astype(str)

        print(f"Limpeza concluída. Restam {len(df)} linhas após a limpeza.\n")

        # 4. Salvar no banco de dados SQLite
        conn = sqlite3.connect(db_path)
        print(f"Conectado ao banco de dados SQLite '{db_path}'.\n")

        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Dados inseridos na tabela '{table_name}' com sucesso.\n")

        count_in_db = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", conn).iloc[0, 0]
        print(f"Total de {count_in_db} linhas na tabela '{table_name}' do banco de dados.\n")

    except Exception as e:
        print(f"Ocorreu um erro: {e}\n")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com o banco de dados fechada.\n")

if __name__ == "__main__":
    create_db_from_csv(CSV_FILE_PATH, DB_FILE_PATH, TABLE_NAME, COLUMNS_TO_USE)