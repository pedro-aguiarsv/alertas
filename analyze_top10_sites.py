#!/usr/bin/env python3
"""
Script para analisar automaticamente os top 10 sites com mais requests
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

def get_top10_sites_with_requests(client, db, start_date, end_date):
    """Busca os top 10 sites com mais requests."""
    print(f"🔍 Buscando top 10 sites com requests de {start_date} a {end_date}...")
    
    query = f"""
        SELECT 
            site_id,
            domain,
            SUM(ad_exchange_total_requests) as total_requests
        FROM {db}.gam_ecpms
        WHERE date >= '{start_date}' 
          AND date <= '{end_date}'
          AND ad_exchange_total_requests > 0
        GROUP BY site_id, domain
        ORDER BY total_requests DESC
        LIMIT 10
    """
    
    try:
        df = client.query_df(query)
        print(f"✅ Encontrados {len(df)} sites (top 10)")
        return df
    except Exception as e:
        print(f"❌ ERRO ao buscar dados: {e}")
        return pd.DataFrame()

def get_detailed_requests_data(client, db, start_date, end_date, site_ids):
    """Busca dados detalhados de requests para os sites selecionados."""
    print(f"📊 Buscando dados detalhados para {len(site_ids)} sites...")
    
    site_filter = f"AND site_id IN ({','.join(map(str, site_ids))})"
    
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
        ORDER BY total_requests DESC
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
    print("🚀 ANÁLISE TOP 10 SITES CLICKHOUSE")
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
        
        # Buscar top 10 sites
        top10_df = get_top10_sites_with_requests(client, config["db"], yesterday, yesterday)
        
        if top10_df.empty:
            print("❌ Nenhum site com requests encontrado para ontem")
            return 1
        
        # Mostrar top 10 sites
        print(f"\n🏆 TOP 10 SITES COM MAIS REQUESTS:")
        print("=" * 80)
        print(f"{'#':<3} {'Site ID':<8} {'Domain':<35} {'Requests':<12}")
        print("-" * 80)
        
        for idx, row in top10_df.iterrows():
            print(f"{idx+1:<3} {row['site_id']:<8} {row['domain'][:34]:<35} {row['total_requests']:<12,.0f}")
        
        print("=" * 80)
        
        # Buscar dados detalhados para os top 10
        site_ids = top10_df['site_id'].tolist()
        detailed_df = get_detailed_requests_data(client, config["db"], yesterday, yesterday, site_ids)
        
        if detailed_df.empty:
            print("❌ Nenhum dado detalhado encontrado")
            return 1
        
        # Salvar resultados
        date_str = yesterday.strftime("%Y%m%d")
        filename = f"top10_sites_requests_{date_str}.csv"
        save_results(detailed_df, filename)
        
        # Mostrar resumo
        print(f"\n📊 RESUMO DOS TOP 10 SITES:")
        print(f"   - Sites analisados: {detailed_df['site_id'].nunique()}")
        print(f"   - Domínios únicos: {detailed_df['domain'].nunique()}")
        print(f"   - Total de requests: {detailed_df['total_requests'].sum():,.0f}")
        print(f"   - Média de requests por site: {detailed_df['total_requests'].mean():,.0f}")
        
        # Estatísticas detalhadas
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"   - Site com mais requests: {detailed_df.loc[detailed_df['total_requests'].idxmax(), 'domain']} ({detailed_df['total_requests'].max():,.0f})")
        print(f"   - Site com menos requests: {detailed_df.loc[detailed_df['total_requests'].idxmin(), 'domain']} ({detailed_df['total_requests'].min():,.0f})")
        
        # Mostrar todos os sites analisados
        print(f"\n📋 TODOS OS SITES ANALISADOS:")
        for idx, row in detailed_df.groupby('site_id')['total_requests'].sum().sort_values(ascending=False).items():
            domain = detailed_df[detailed_df['site_id'] == idx]['domain'].iloc[0]
            print(f"   - Site {idx} ({domain}): {row:,.0f} requests")
        
        print("\n🎉 Análise concluída!")
        print(f"\n💡 PRÓXIMO PASSO:")
        print("   Configure o token do Plausible para cruzar com dados de visitors")
        print(f"   Arquivo salvo: {filename}")
        
    except Exception as e:
        print(f"💥 ERRO: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
