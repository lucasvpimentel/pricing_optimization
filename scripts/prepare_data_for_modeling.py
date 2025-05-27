# scripts/prepare_data_for_modeling.py

import pandas as pd
import sqlite3
from pathlib import Path
from load_data_from_db import load_data_from_db # Importa a função do script anterior

# --- Configurações ---
BASE_DIR = Path(__file__).resolve().parent.parent
DB_FILE_PATH = BASE_DIR / 'data' / 'ecommerce.db'
DB_TABLE_NAME_RAW = 'transactions' # Nome da tabela de dados brutos
TABLE_NAME_MONTHLY_DATA = 'sku_monthly_country_data' # Novo nome da tabela para dados agregados no DB
OUTPUT_CSV_PATH = BASE_DIR / 'data' / 'sku_monthly_country_data.csv' # Caminho para o CSV de saída

def prepare_monthly_sku_country_data(db_path: Path, db_table_name_raw: str, output_csv_path: Path, table_name_monthly_data: str):
    """
    Carrega dados brutos, calcula preço médio ponderado mensal e quantidade vendida mensal
    por SKU e País, e salva o resultado em um CSV e em uma nova tabela no DB.
    """
    print("Iniciando a preparação dos dados mensais por SKU e País...\n")

    # 1. Carregar os dados do SQLite
    df = load_data_from_db(db_path, db_table_name_raw)

    if df is None:
        print("Erro: Não foi possível carregar os dados do banco de dados. Encerrando.\n")
        return

    print("Dados carregados com sucesso. Iniciando agregação...\n")
    print(f"Número de linhas antes da agregação: {len(df)}\n")

    # 2. Extrair Ano e Mês da 'InvoiceDate' (sem considerar a hora)
    #    .dt.to_period('M') já faz isso, convertendo para um período mensal.
    df['InvoiceYearMonth'] = df['InvoiceDate'].dt.to_period('M')

    # 3. Calcular a Receita Total por linha de transação (SKU * Quantidade)
    #    Isso é um passo intermediário necessário para o preço médio PONDERADO.
    df['TotalRevenuePerItem'] = df['Quantity'] * df['UnitPrice']

    # 4. Agrupar por StockCode, Mês e País para calcular métricas mensais
    df_monthly_sku_country = df.groupby(['StockCode', 'InvoiceYearMonth', 'Country']).agg(
        total_quantity=('Quantity', 'sum'),
        total_revenue=('TotalRevenuePerItem', 'sum') # Agrega a receita total para o cálculo ponderado
    ).reset_index()

    # Filtra grupos onde a quantidade total é zero para evitar divisão por zero no preço
    df_monthly_sku_country = df_monthly_sku_country[df_monthly_sku_country['total_quantity'] > 0]

    # 5. Calcular o Preço Médio Ponderado Mensal
    #    Preço Médio Ponderado = Receita Total / Quantidade Total
    df_monthly_sku_country['avg_price_monthly'] = df_monthly_sku_country['total_revenue'] / df_monthly_sku_country['total_quantity']

    # 6. Remover a coluna 'total_revenue' se não for necessária no output final
    df_monthly_sku_country.drop(columns=['total_revenue'], inplace=True)

    # Converter 'InvoiceYearMonth' de Period para Timestamp (primeiro dia do mês)
    # E então para string no formato YYYY-MM para facilitar o armazenamento no DB
    # e manter a compatibilidade com o CSV, se necessário
    df_monthly_sku_country['InvoiceYearMonth'] = df_monthly_sku_country['InvoiceYearMonth'].dt.strftime('%Y-%m')


    # 7. Ordenar os dados para séries temporais e melhor visualização
    df_monthly_sku_country.sort_values(by=['StockCode', 'Country', 'InvoiceYearMonth'], inplace=True)

    print("Agregação mensal por SKU e País concluída.\n")
    print("Exemplo das primeiras linhas dos dados agregados:\n")
    print(df_monthly_sku_country.head())
    print(f"\nTotal de {len(df_monthly_sku_country)} linhas de dados mensais agregados.\n")
    print(f"Colunas do DataFrame final: {df_monthly_sku_country.columns.tolist()}\n")

    # 8. Salvar os dados agregados em um novo CSV
    try:
        output_csv_path.parent.mkdir(parents=True, exist_ok=True) # Garante que a pasta 'data' exista
        df_monthly_sku_country.to_csv(output_csv_path, index=False)
        print(f"Dados mensais por SKU e País salvos em '{output_csv_path}' com sucesso.\n")
    except Exception as e:
        print(f"Erro ao salvar o CSV: {e}\n")

    # 9. Salvar os dados agregados em uma nova tabela no banco de dados SQLite
    conn = None # Inicializa conn como None
    try:
        conn = sqlite3.connect(db_path)
        print(f"Conectado ao banco de dados SQLite '{db_path}' para salvar dados agregados.\n")

        # Usa 'replace' para recriar a tabela a cada execução, ou 'append' para adicionar
        df_monthly_sku_country.to_sql(table_name_monthly_data, conn, if_exists='replace', index=False)
        print(f"Dados mensais por SKU e País inseridos na tabela '{table_name_monthly_data}' com sucesso.\n")

        # Verificar a contagem de linhas no DB para confirmar
        count_in_db = pd.read_sql(f"SELECT COUNT(*) FROM {table_name_monthly_data}", conn).iloc[0, 0]
        print(f"Total de {count_in_db} linhas na tabela '{table_name_monthly_data}' do banco de dados.\n")

    except Exception as e:
        print(f"Ocorreu um erro ao salvar no banco de dados: {e}\n")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados para dados agregados fechada.\n")

if __name__ == "__main__":
    prepare_monthly_sku_country_data(DB_FILE_PATH, DB_TABLE_NAME_RAW, OUTPUT_CSV_PATH, TABLE_NAME_MONTHLY_DATA)