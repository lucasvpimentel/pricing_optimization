# scripts/load_data_from_db.py

import pandas as pd
import sqlite3
from pathlib import Path

# --- Configurações ---
# Define o diretório base do projeto (um nível acima de 'scripts')
BASE_DIR = Path(__file__).resolve().parent.parent

# Caminho do arquivo de banco de dados usando pathlib
DB_FILE_PATH = BASE_DIR / 'data' / 'ecommerce.db'
TABLE_NAME = 'transactions'

def load_data_from_db(db_path: Path, table_name: str):
    """
    Carrega todos os dados de uma tabela SQLite para um Pandas DataFrame.
    """
    print(f"Tentando carregar dados da tabela '{table_name}' do banco de dados '{db_path}'...")

    if not db_path.exists():
        print(f"Erro: O arquivo do banco de dados '{db_path}' não foi encontrado. Por favor, execute 'create_ecommerce_db.py' primeiro.")
        return None

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print(f"Conectado ao banco de dados SQLite '{db_path}'.")

        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)

        # Re-converter 'InvoiceDate' para datetime
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

        print(f"Dados carregados com sucesso. Total de {len(df)} linhas.")
        return df

    except Exception as e:
        print(f"Ocorreu um erro ao carregar os dados: {e}")
        return None
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    df_ecommerce = load_data_from_db(DB_FILE_PATH, TABLE_NAME)

    if df_ecommerce is not None:
        print("\nPrimeiras 5 linhas do DataFrame carregado:")
        print(df_ecommerce.head())
        print("\nInformações do DataFrame:")
        df_ecommerce.info()