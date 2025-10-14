#!/usr/bin/env python3
"""
Script para cruzar dados de ad_exchange_total_requests com API do Plausible (visitors)
"""

import os
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urlparse
import clickhouse_connect
from dotenv import load_dotenv
import time

# ===== CONFIG =====
PLAUSIBLE_API_BASE = "https://plausible.io/api/v1"
LOOKBACK_DAYS = 7  # Quantos dias buscar no passado
# ==================

def get_db_config():
    """Carrega e retorna as configurações do banco e Plausible."""
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
    
    config = {
        "url": os.getenv("CLICKHOUSE_URL"),
        "db":  os.getenv("CLICKHOUSE_DATABASE"),
        "usr": os.getenv("CLICKHOUSE_USER"),
        "pwd": os.getenv("CLICKHOUSE_PASSWORD"),
        "plausible_token": os.getenv("PLAUSIBLE_API_TOKEN"),
        "plausible_site_id": os.getenv("PLAUSIBLE_SITE_ID"),
    }
    
    missing_vars = []
    if not config["url"]: missing_vars.append("CLICKHOUSE_URL")
    if not config["db"]: missing_vars.append("CLICKHOUSE_DATABASE") 
    if not config["usr"]: missing_vars.append("CLICKHOUSE_USER")
    if not config["pwd"]: missing_vars.append("CLICKHOUSE_PASSWORD")
    if not config["plausible_token"]: missing_vars.append("PLAUSIBLE_API_TOKEN")
    if not config["plausible_site_id"]: missing_vars.append("PLAUSIBLE_SITE_ID")
    
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

def get_requests_data(client, db, start_date, end_date):
    """Busca dados de ad_exchange_total_requests do ClickHouse."""
    print(f"🔍 Buscando dados de requests de {start_date} a {end_date}...")
    
    # Query para buscar requests por site e data
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
        GROUP BY site_id, date, domain
        ORDER BY site_id, date
    """
    
    try:
        df = client.query_df(query)
        print(f"✅ Encontrados {len(df)} registros de requests")
        return df
    except Exception as e:
        print(f"❌ ERRO ao buscar dados de requests: {e}")
        return pd.DataFrame()

def get_plausible_visitors(config, start_date, end_date, site_domain=None):
    """Busca dados de visitors do Plausible."""
    print(f"📊 Buscando dados de visitors do Plausible de {start_date} a {end_date}...")
    
    headers = {
        "Authorization": f"Bearer {config['plausible_token']}",
        "Content-Type": "application/json"
    }
    
    # Parâmetros da API
    params = {
        "site_id": config["plausible_site_id"],
        "period": "custom",
        "date": f"{start_date},{end_date}",
        "metrics": "visitors",
        "interval": "date"
    }
    
    # Se especificou um domínio específico, filtrar por ele
    if site_domain:
        params["filters"] = f"event:page=={site_domain}"
    
    try:
        response = requests.get(
            f"{PLAUSIBLE_API_BASE}/stats/timeseries",
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        visitors_data = []
        
        for item in data.get("results", []):
            visitors_data.append({
                "date": item["date"],
                "visitors": item["visitors"]
            })
        
        df = pd.DataFrame(visitors_data)
        df["date"] = pd.to_datetime(df["date"])
        
        print(f"✅ Encontrados {len(df)} registros de visitors")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO na API do Plausible: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status: {e.response.status_code}")
            print(f"   Response: {e.response.text}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERRO geral na busca de visitors: {e}")
        return pd.DataFrame()

def get_all_plausible_visitors(config, start_date, end_date):
    """Busca dados de visitors para todos os domínios."""
    print(f"📊 Buscando visitors para todos os domínios...")
    
    headers = {
        "Authorization": f"Bearer {config['plausible_token']}",
        "Content-Type": "application/json"
    }
    
    params = {
        "site_id": config["plausible_site_id"],
        "period": "custom", 
        "date": f"{start_date},{end_date}",
        "metrics": "visitors,page",
        "limit": 1000  # Ajustar conforme necessário
    }
    
    try:
        response = requests.get(
            f"{PLAUSIBLE_API_BASE}/stats/breakdown",
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        visitors_data = []
        
        for item in data.get("results", []):
            page = item.get("page", "")
            if page:
                # Extrair domínio da página
                domain = page.split("/")[0] if "/" in page else page
                visitors_data.append({
                    "domain": domain,
                    "visitors": item["visitors"],
                    "page": page
                })
        
        df = pd.DataFrame(visitors_data)
        
        # Agrupar por domínio
        df_grouped = df.groupby("domain")["visitors"].sum().reset_index()
        
        print(f"✅ Encontrados visitors para {len(df_grouped)} domínios")
        return df_grouped
        
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO na API do Plausible (breakdown): {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERRO geral na busca de visitors por domínio: {e}")
        return pd.DataFrame()

def cross_data(requests_df, visitors_df):
    """Cruza os dados de requests com visitors."""
    print("🔄 Cruzando dados de requests com visitors...")
    
    if requests_df.empty or visitors_df.empty:
        print("❌ Dados insuficientes para cruzamento")
        return pd.DataFrame()
    
    # Converter data para string para fazer o merge
    requests_df["date_str"] = requests_df["date"].astype(str)
    
    # Fazer merge por domínio e data
    merged_df = requests_df.merge(
        visitors_df, 
        left_on=["domain", "date_str"],
        right_on=["domain", "date"],
        how="left",
        suffixes=("_requests", "_visitors")
    )
    
    # Calcular métricas
    merged_df["requests_per_visitor"] = merged_df["total_requests"] / merged_df["visitors"]
    merged_df["visitors_per_request"] = merged_df["visitors"] / merged_df["total_requests"]
    
    # Limpar dados infinitos
    merged_df = merged_df.replace([float('inf'), -float('inf')], None)
    
    print(f"✅ Cruzamento concluído: {len(merged_df)} registros")
    return merged_df

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
    print("🚀 INTEGRAÇÃO CLICKHOUSE + PLAUSIBLE")
    print("=" * 60)
    
    try:
        # Configurações
        config = get_db_config()
        print(f"✅ Configurações carregadas")
        
        # Conectar ao banco
        client = get_client_db(config)
        print("✅ Conectado ao ClickHouse")
        
        # Definir período
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=LOOKBACK_DAYS)
        
        print(f"📅 Período: {start_date} a {end_date}")
        
        # Buscar dados de requests
        requests_df = get_requests_data(client, config["db"], start_date, end_date)
        
        if requests_df.empty:
            print("❌ Nenhum dado de requests encontrado")
            return 1
        
        # Buscar dados de visitors
        visitors_df = get_all_plausible_visitors(config, start_date, end_date)
        
        if visitors_df.empty:
            print("❌ Nenhum dado de visitors encontrado")
            return 1
        
        # Cruzar dados
        merged_df = cross_data(requests_df, visitors_df)
        
        # Salvar resultados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"requests_vs_visitors_{timestamp}.csv"
        save_results(merged_df, filename)
        
        # Mostrar resumo
        if not merged_df.empty:
            print(f"\n📊 RESUMO:")
            print(f"   - Sites analisados: {merged_df['site_id'].nunique()}")
            print(f"   - Domínios únicos: {merged_df['domain'].nunique()}")
            print(f"   - Total de requests: {merged_df['total_requests'].sum():,.0f}")
            print(f"   - Total de visitors: {merged_df['visitors'].sum():,.0f}")
            
            # Top 10 sites por requests
            top_sites = merged_df.groupby("site_id")["total_requests"].sum().sort_values(ascending=False).head(10)
            print(f"\n🏆 TOP 10 SITES POR REQUESTS:")
            for site_id, requests in top_sites.items():
                print(f"   - Site {site_id}: {requests:,.0f} requests")
        
        print("\n🎉 Análise concluída!")
        
    except Exception as e:
        print(f"💥 ERRO: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
