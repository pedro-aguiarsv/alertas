#!/usr/bin/env python3
"""
Script para explorar a estrutura das tabelas do banco de dados
"""

import os
from pathlib import Path
from urllib.parse import urlparse
import clickhouse_connect
from dotenv import load_dotenv

def get_db_config():
    """Carrega e retorna as configurações do banco."""
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
    
    config = {
        "url": os.getenv("CLICKHOUSE_URL"),
        "db":  os.getenv("CLICKHOUSE_DATABASE"),
        "usr": os.getenv("CLICKHOUSE_USER"),
        "pwd": os.getenv("CLICKHOUSE_PASSWORD"),
    }
    
    if not all([config["url"], config["db"], config["usr"], config["pwd"]]):
        raise RuntimeError("Faltam variáveis do banco de dados no .env (CLICKHOUSE_URL/DATABASE/USER/PASSWORD)")
    
    return config

def get_client_db(config):
    """Cria e retorna um cliente de banco de dados."""
    p = urlparse(config["url"])
    client = clickhouse_connect.get_client(
        host=p.hostname, port=(p.port or 8123),
        username=config["usr"], password=config["pwd"], database=config["db"],
        secure=(p.scheme == "https"),
        settings={"readonly": 1},
    )
    return client

def show_table_structure(client, db, table_name):
    """Mostra a estrutura de uma tabela."""
    print(f"\n{'='*60}")
    print(f"📋 ESTRUTURA DA TABELA: {table_name}")
    print(f"{'='*60}")
    
    try:
        # Mostrar estrutura da tabela
        schema_sql = f"DESCRIBE {db}.{table_name}"
        schema_df = client.query_df(schema_sql)
        
        print(f"\n🔍 COLUNAS ({len(schema_df)} encontradas):")
        print("-" * 80)
        print(f"{'Nome':<30} {'Tipo':<25} {'Comentário':<20}")
        print("-" * 80)
        
        for _, row in schema_df.iterrows():
            name = row['name']
            type_info = row['type']
            comment = row.get('comment', '') if 'comment' in row else ''
            print(f"{name:<30} {type_info:<25} {comment:<20}")
        
        # Mostrar contagem de linhas
        count_sql = f"SELECT count(*) as total FROM {db}.{table_name}"
        count_result = client.query_df(count_sql)
        total_rows = count_result.iloc[0]['total']
        print(f"\n📊 Total de linhas: {total_rows:,}")
        
        # Mostrar amostra dos dados
        sample_sql = f"SELECT * FROM {db}.{table_name} LIMIT 5"
        sample_df = client.query_df(sample_sql)
        
        print(f"\n📋 AMOSTRA DOS DADOS (5 linhas):")
        print("-" * 100)
        
        if not sample_df.empty:
            # Mostrar nomes das colunas
            print("Colunas:", " | ".join(sample_df.columns.tolist()))
            print("-" * 100)
            
            # Mostrar dados
            for idx, row in sample_df.iterrows():
                values = []
                for col in sample_df.columns:
                    val = row[col]
                    if val is None:
                        val = "NULL"
                    elif isinstance(val, str) and len(str(val)) > 20:
                        val = str(val)[:17] + "..."
                    values.append(str(val))
                print(f"Linha {idx+1}: {' | '.join(values)}")
        else:
            print("Nenhum dado encontrado na tabela.")
            
    except Exception as e:
        print(f"❌ ERRO ao analisar tabela {table_name}: {e}")

def show_table_info(client, db):
    """Mostra informações sobre todas as tabelas."""
    print(f"\n{'='*60}")
    print(f"📊 INFORMAÇÕES GERAIS DO BANCO: {db}")
    print(f"{'='*60}")
    
    try:
        # Listar todas as tabelas
        tables_sql = "SHOW TABLES"
        tables_df = client.query_df(tables_sql)
        
        print(f"\n🗂️  TABELAS DISPONÍVEIS ({len(tables_df)} encontradas):")
        print("-" * 50)
        
        for _, row in tables_df.iterrows():
            table_name = row['name']
            print(f"  - {table_name}")
        
        return tables_df['name'].tolist()
        
    except Exception as e:
        print(f"❌ ERRO ao listar tabelas: {e}")
        return []

def main():
    print("🚀 EXPLORADOR DE BANCO DE DADOS")
    print("=" * 60)
    
    try:
        # Conectar ao banco
        config = get_db_config()
        print(f"✅ Conectando ao banco: {config['db']}")
        
        client = get_client_db(config)
        print("✅ Conexão estabelecida!")
        
        # Mostrar informações gerais
        tables = show_table_info(client, config['db'])
        
        # Analisar tabelas específicas de interesse
        tables_of_interest = ['gam_impressions', 'gads_costs']
        
        for table in tables_of_interest:
            if table in tables:
                show_table_structure(client, config['db'], table)
            else:
                print(f"\n⚠️  Tabela '{table}' não encontrada no banco.")
        
        # Opção de analisar outras tabelas
        print(f"\n{'='*60}")
        print("💡 DICA: Para analisar outras tabelas, edite o script")
        print("   e adicione o nome da tabela em 'tables_of_interest'")
        print(f"{'='*60}")
        
        print("\n🎉 Análise concluída!")
        
    except Exception as e:
        print(f"💥 ERRO: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
