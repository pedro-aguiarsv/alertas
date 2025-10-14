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
PLAUSIBLE_API_BASE = "https://wearenalytics.com/api/v1"
LOOKBACK_DAYS = 1  # Buscar apenas dados de ontem
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
    }
    
    missing_vars = []
    if not config["url"]: missing_vars.append("CLICKHOUSE_URL")
    if not config["db"]: missing_vars.append("CLICKHOUSE_DATABASE") 
    if not config["usr"]: missing_vars.append("CLICKHOUSE_USER")
    if not config["pwd"]: missing_vars.append("CLICKHOUSE_PASSWORD")
    if not config["plausible_token"]: missing_vars.append("PLAUSIBLE_API_TOKEN")
    
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
    if start_date == end_date:
        print(f"🔍 Buscando dados de requests de {start_date}...")
    else:
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
    if start_date == end_date:
        print(f"📊 Buscando dados de visitors do Plausible de {start_date}...")
    else:
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

def detect_plausible_api_version(config):
    """Detecta a versão correta da API do Plausible."""
    print("🔍 Detectando versão da API do Plausible...")
    
    headers = {
        "Authorization": f"Bearer {config['plausible_token']}",
        "Content-Type": "application/json"
    }
    
    # Tentar diferentes URLs possíveis
    api_urls = [
        "https://wearenalytics.com/api/v1",
        "https://wearenalytics.com/api/v2", 
        "https://plausible.io/api/v1",
        "https://plausible.io/api/v2"
    ]
    
    for api_url in api_urls:
        try:
            print(f"   Testando: {api_url}/sites")
            response = requests.get(
                f"{api_url}/sites",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                print(f"✅ API encontrada: {api_url}")
                return api_url
            elif response.status_code == 404:
                print(f"   ❌ 404 - URL não encontrada")
                continue
            else:
                print(f"   ⚠️  Status {response.status_code}")
                continue
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Erro: {e}")
            continue
    
    print("❌ Nenhuma API válida encontrada")
    return None

def get_all_sites_from_plausible(config):
    """Busca todos os sites da conta Plausible."""
    print("📋 Buscando todos os sites da conta Plausible...")
    
    # Detectar API correta
    api_base = detect_plausible_api_version(config)
    if not api_base:
        return []
    
    headers = {
        "Authorization": f"Bearer {config['plausible_token']}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(
            f"{api_base}/sites",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        sites = []
        
        for site in data:
            sites.append({
                "site_id": site["domain"],
                "domain": site["domain"]
            })
        
        print(f"✅ Encontrados {len(sites)} sites na conta Plausible")
        return sites
        
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO ao buscar sites do Plausible: {e}")
        return []

def get_visitors_for_all_sites(config, sites, start_date, end_date, api_base):
    """Busca dados de visitors para todos os sites da conta."""
    if start_date == end_date:
        print(f"📊 Buscando visitors para {len(sites)} sites de {start_date}...")
    else:
        print(f"📊 Buscando visitors para {len(sites)} sites...")
    
    headers = {
        "Authorization": f"Bearer {config['plausible_token']}",
        "Content-Type": "application/json"
    }
    
    all_visitors_data = []
    
    for i, site in enumerate(sites, 1):
        site_domain = site["domain"]
        print(f"   [{i}/{len(sites)}] Processando {site_domain}...")
        
        params = {
            "site_id": site_domain,
            "period": "custom", 
            "date": f"{start_date},{end_date}",
            "metrics": "visitors"
        }
        
        try:
            response = requests.get(
                f"{api_base}/stats/timeseries",
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Somar todos os visitors do período
            total_visitors = sum(item["visitors"] for item in data.get("results", []))
            
            all_visitors_data.append({
                "domain": site_domain,
                "visitors": total_visitors
            })
            
            # Pequena pausa para não sobrecarregar a API
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"      ⚠️  Erro ao buscar dados de {site_domain}: {e}")
            # Adicionar com 0 visitors em caso de erro
            all_visitors_data.append({
                "domain": site_domain,
                "visitors": 0
            })
        except Exception as e:
            print(f"      ⚠️  Erro geral para {site_domain}: {e}")
            all_visitors_data.append({
                "domain": site_domain,
                "visitors": 0
            })
    
    df = pd.DataFrame(all_visitors_data)
    print(f"✅ Processados {len(df)} sites")
    return df

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
        
        # Definir período (apenas ontem)
        yesterday = datetime.now().date() - timedelta(days=1)
        
        print(f"📅 Analisando dados de: {yesterday} (ontem)")
        
        # Buscar dados de requests
        requests_df = get_requests_data(client, config["db"], yesterday, yesterday)
        
        if requests_df.empty:
            print("❌ Nenhum dado de requests encontrado para ontem")
            return 1
        
        # Buscar todos os sites da conta Plausible
        all_sites = get_all_sites_from_plausible(config)
        
        if not all_sites:
            print("❌ Nenhum site encontrado na conta Plausible")
            return 1
        
        # Detectar API para usar nas próximas chamadas
        api_base = detect_plausible_api_version(config)
        if not api_base:
            print("❌ Não foi possível detectar a API do Plausible")
            return 1
        
        # Buscar dados de visitors para todos os sites
        visitors_df = get_visitors_for_all_sites(config, all_sites, yesterday, yesterday, api_base)
        
        if visitors_df.empty:
            print("❌ Nenhum dado de visitors encontrado para ontem")
            return 1
        
        # Cruzar dados
        merged_df = cross_data(requests_df, visitors_df)
        
        # Salvar resultados
        date_str = yesterday.strftime("%Y%m%d")
        filename = f"requests_vs_visitors_{date_str}.csv"
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
