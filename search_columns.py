#!/usr/bin/env python3
"""
Script para buscar colunas que contenham uma palavra específica
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

def search_columns_with_keyword(client, db, keyword):
    """Busca colunas que contenham uma palavra específica em todas as tabelas."""
    print(f"\n🔍 BUSCANDO COLUNAS QUE CONTENHAM: '{keyword.upper()}'")
    print("=" * 80)
    
    try:
        # Listar todas as tabelas
        tables_sql = "SHOW TABLES"
        tables_df = client.query_df(tables_sql)
        tables = tables_df['name'].tolist()
        
        found_columns = []
        
        for table in tables:
            try:
                # Obter estrutura da tabela
                schema_sql = f"DESCRIBE {db}.{table}"
                schema_df = client.query_df(schema_sql)
                
                # Buscar colunas que contenham a palavra-chave
                matching_columns = schema_df[
                    schema_df['name'].str.contains(keyword, case=False, na=False)
                ]
                
                if not matching_columns.empty:
                    print(f"\n📋 TABELA: {table}")
                    print("-" * 60)
                    
                    for _, row in matching_columns.iterrows():
                        column_info = f"  • {row['name']} ({row['type']})"
                        print(column_info)
                        found_columns.append({
                            'table': table,
                            'column': row['name'],
                            'type': row['type']
                        })
                        
            except Exception as e:
                print(f"❌ Erro ao analisar tabela {table}: {e}")
        
        # Resumo
        print(f"\n{'='*80}")
        print(f"📊 RESUMO DA BUSCA POR '{keyword.upper()}'")
        print(f"{'='*80}")
        
        if found_columns:
            print(f"✅ Encontradas {len(found_columns)} colunas com '{keyword}':")
            print()
            
            # Agrupar por tabela
            tables_with_matches = {}
            for col in found_columns:
                table = col['table']
                if table not in tables_with_matches:
                    tables_with_matches[table] = []
                tables_with_matches[table].append(col)
            
            for table, columns in tables_with_matches.items():
                print(f"📋 {table} ({len(columns)} colunas):")
                for col in columns:
                    print(f"   - {col['column']} ({col['type']})")
                print()
        else:
            print(f"❌ Nenhuma coluna encontrada contendo '{keyword}'")
        
        return found_columns
        
    except Exception as e:
        print(f"💥 ERRO na busca: {e}")
        return []

def main():
    print("🔍 BUSCADOR DE COLUNAS POR PALAVRA-CHAVE")
    print("=" * 60)
    
    try:
        # Conectar ao banco
        config = get_db_config()
        print(f"✅ Conectando ao banco: {config['db']}")
        
        client = get_client_db(config)
        print("✅ Conexão estabelecida!")
        
        # Buscar por "request"
        keyword = "request"
        found_columns = search_columns_with_keyword(client, config['db'], keyword)
        
        print("\n🎉 Busca concluída!")
        
        # Opção de buscar outras palavras
        print(f"\n💡 DICA: Para buscar outras palavras, edite o script")
        print(f"   e altere a variável 'keyword' na linha 85")
        
    except Exception as e:
        print(f"💥 ERRO: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
