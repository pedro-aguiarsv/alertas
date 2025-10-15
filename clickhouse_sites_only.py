#!/usr/bin/env python3
"""
Script para listar sites do ClickHouse e analisar dados de requests
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
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
    
    missing_vars = []
    if not config["url"]: missing_vars.append("CLICKHOUSE_URL")
    if not config["db"]: missing_vars.append("CLICKHOUSE_DATABASE") 
    if not config["usr"]: missing_vars.append("CLICKHOUSE_USER")
    if not config["pwd"]: missing_vars.append("CLICKHOUSE_PASSWORD")
    
    if missing_vars:
        raise RuntimeError(f"Faltam variáveis no .env: {', '.join(missing_vars)}")
    
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

def list_sites_with_requests(client, db, start_date, end_date):
    """Lista todos os sites que tiveram requests no período."""
    print(f"🔍 Buscando sites com requests de {start_date} a {end_date}...")
    
    query = f"""
        SELECT 
            site_id,
            domain,
            SUM(ad_exchange_total_requests) as total_requests,
            COUNT(DISTINCT date) as days_with_data
        FROM {db}.gam_ecpms
        WHERE date >= '{start_date}' 
          AND date <= '{end_date}'
          AND ad_exchange_total_requests > 0
        GROUP BY site_id, domain
        ORDER BY total_requests DESC
    """
    
    try:
        df = client.query_df(query)
        print(f"✅ Encontrados {len(df)} sites com requests")
        return df
    except Exception as e:
        print(f"❌ ERRO ao buscar dados: {e}")
        return pd.DataFrame()

def get_detailed_requests_data(client, db, start_date, end_date, selected_sites=None):
    """Busca dados detalhados de requests."""
    print(f"📊 Buscando dados detalhados de requests...")
    
    if selected_sites:
        site_filter = f"AND site_id IN ({','.join(map(str, selected_sites))})"
    else:
        site_filter = ""
    
    query = f"""
        SELECT 
            site_id,
            date,
            domain,
            SUM(ad_exchange_total_requests) as total_requests
        FROM {db}.gam_ecpms
        WHERE date >= '{start_date}' 
          AND date <= '{end_date}'
          AND ad_exchange_total_requests > 0
          {site_filter}
        GROUP BY site_id, date, domain
        ORDER BY site_id, date
    """
    
    try:
        df = client.query_df(query)
        print(f"✅ Encontrados {len(df)} registros detalhados")
        return df
    except Exception as e:
        print(f"❌ ERRO ao buscar dados detalhados: {e}")
        return pd.DataFrame()

def save_results(df, filename):
    """Salva os resultados em CSV."""
    if df.empty:
        print("❌ Nenhum dado para salvar")
        return
    
    filepath = Path(__file__).with_name(filename)
    df.to_csv(filepath, index=False)
    print(f"💾 Dados salvos em: {filename}")
    print(f"   - Total de registros: {len(df)}")
    print(f"   - Colunas: {list(df.columns)}")

def main():
    print("🚀 ANÁLISE DE SITES CLICKHOUSE")
    print("=" * 60)
    
    try:
        # Configurações
        config = get_db_config()
        print(f"✅ Configurações carregadas")
        
        # Conectar ao banco
        client = get_client_db(config)
        print("✅ Conectado ao ClickHouse")
        
        # Definir período (apenas ontem)
        yesterday = datetime.now().date() - timedelta(days=1)
        print(f"📅 Analisando dados de: {yesterday} (ontem)")
        
        # Listar sites com requests
        sites_df = list_sites_with_requests(client, config["db"], yesterday, yesterday)
        
        if sites_df.empty:
            print("❌ Nenhum site com requests encontrado para ontem")
            return 1
        
        # Mostrar sites disponíveis
        print(f"\n📊 SITES COM REQUESTS ({len(sites_df)} encontrados):")
        print("=" * 80)
        print(f"{'#':<3} {'Site ID':<8} {'Domain':<30} {'Requests':<12} {'Dias':<6}")
        print("-" * 80)
        
        for idx, row in sites_df.iterrows():
            print(f"{idx+1:<3} {row['site_id']:<8} {row['domain'][:29]:<30} {row['total_requests']:<12,.0f} {row['days_with_data']:<6}")
        
        print("=" * 80)
        
        # Seleção de sites
        print(f"\n🎯 SELECIONE OS SITES PARA ANÁLISE DETALHADA:")
        print("Opções:")
        print("  - Digite números separados por vírgula: 1,3,5")
        print("  - Digite 'all' para selecionar todos")
        print("  - Digite 'top10' para os 10 com mais requests")
        print("  - Digite 'quit' para sair")
        
        while True:
            selection = input("\nSua escolha: ").strip()
            
            if selection.lower() == 'quit':
                print("👋 Saindo...")
                return 0
            
            selected_sites = None
            
            if selection.lower() == 'all':
                selected_sites = sites_df['site_id'].tolist()
                break
            
            if selection.lower() == 'top10':
                selected_sites = sites_df.head(10)['site_id'].tolist()
                break
            
            try:
                indices = [int(x.strip()) for x in selection.split(',')]
                selected_sites = []
                
                for idx in indices:
                    if 1 <= idx <= len(sites_df):
                        selected_sites.append(sites_df.iloc[idx-1]['site_id'])
                    else:
                        print(f"❌ Índice {idx} inválido (deve estar entre 1 e {len(sites_df)})")
                
                if selected_sites:
                    break
                else:
                    print("❌ Nenhum site válido selecionado")
                    
            except ValueError:
                print("❌ Formato inválido. Use números separados por vírgula.")
        
        print(f"\n✅ Selecionados {len(selected_sites)} sites:")
        for site_id in selected_sites:
            site_info = sites_df[sites_df['site_id'] == site_id].iloc[0]
            print(f"   - Site {site_id} ({site_info['domain']}): {site_info['total_requests']:,.0f} requests")
        
        # Buscar dados detalhados
        detailed_df = get_detailed_requests_data(client, config["db"], yesterday, yesterday, selected_sites)
        
        if detailed_df.empty:
            print("❌ Nenhum dado detalhado encontrado")
            return 1
        
        # Salvar resultados
        date_str = yesterday.strftime("%Y%m%d")
        filename = f"clickhouse_requests_{date_str}.csv"
        save_results(detailed_df, filename)
        
        # Mostrar resumo
        print(f"\n📊 RESUMO:")
        print(f"   - Sites analisados: {detailed_df['site_id'].nunique()}")
        print(f"   - Domínios únicos: {detailed_df['domain'].nunique()}")
        print(f"   - Total de requests: {detailed_df['total_requests'].sum():,.0f}")
        print(f"   - Registros detalhados: {len(detailed_df)}")
        
        # Top sites por requests
        top_sites = detailed_df.groupby("site_id")["total_requests"].sum().sort_values(ascending=False).head(10)
        print(f"\n🏆 TOP 10 SITES POR REQUESTS:")
        for site_id, requests in top_sites.items():
            domain = detailed_df[detailed_df['site_id'] == site_id]['domain'].iloc[0]
            print(f"   - Site {site_id} ({domain}): {requests:,.0f} requests")
        
        print("\n🎉 Análise concluída!")
        print(f"\n💡 PRÓXIMO PASSO:")
        print("   Configure o token do Plausible para cruzar com dados de visitors")
        
    except Exception as e:
        print(f"💥 ERRO: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
