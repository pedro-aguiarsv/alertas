#!/usr/bin/env python3
"""
Script para listar todos os sites do Plausible e seus visitors de ontem
"""

import os
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

def get_db_config():
    """Carrega e retorna as configurações do Plausible."""
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
    
    config = {
        "plausible_token": os.getenv("PLAUSIBLE_API_TOKEN"),
    }
    
    if not config["plausible_token"]:
        raise RuntimeError("Falta variável PLAUSIBLE_API_TOKEN no .env")
    
    return config

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

def list_all_plausible_sites(config, api_base):
    """Lista todos os sites da conta Plausible."""
    print("📋 Listando todos os sites da conta Plausible...")
    
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
                "domain": site["domain"]
            })
        
        print(f"✅ Encontrados {len(sites)} sites na conta Plausible")
        return sites
        
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO ao buscar sites do Plausible: {e}")
        return []

def get_visitors_for_all_sites(config, sites, date, api_base):
    """Busca dados de visitors para todos os sites de uma data específica."""
    print(f"📊 Buscando visitors para {len(sites)} sites de {date}...")
    
    headers = {
        "Authorization": f"Bearer {config['plausible_token']}",
        "Content-Type": "application/json"
    }
    
    all_visitors_data = []
    
    for i, site in enumerate(sites, 1):
        site_domain = site["domain"]
        print(f"   [{i:3d}/{len(sites)}] Processando {site_domain}...")
        
        params = {
            "site_id": site_domain,
            "period": "day",
            "date": date,
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
            
            # Buscar visitors do dia específico
            visitors_count = 0
            for item in data.get("results", []):
                if item["date"] == date:
                    visitors_count = item["visitors"]
                    break
            
            all_visitors_data.append({
                "domain": site_domain,
                "date": date,
                "visitors": visitors_count
            })
            
            # Pequena pausa para não sobrecarregar a API
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"      ⚠️  Erro ao buscar dados de {site_domain}: {e}")
            # Adicionar com 0 visitors em caso de erro
            all_visitors_data.append({
                "domain": site_domain,
                "date": date,
                "visitors": 0
            })
        except Exception as e:
            print(f"      ⚠️  Erro geral para {site_domain}: {e}")
            all_visitors_data.append({
                "domain": site_domain,
                "date": date,
                "visitors": 0
            })
    
    df = pd.DataFrame(all_visitors_data)
    print(f"✅ Processados {len(df)} sites")
    return df

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
    print("🚀 LISTAGEM DE SITES E VISITORS PLAUSIBLE")
    print("=" * 60)
    
    try:
        # Configurações
        config = get_db_config()
        print(f"✅ Configurações carregadas")
        
        # Detectar API do Plausible
        api_base = detect_plausible_api_version(config)
        if not api_base:
            print("❌ Não foi possível detectar a API do Plausible")
            return 1
        
        # Definir data (ontem)
        yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"📅 Analisando dados de: {yesterday} (ontem)")
        
        # Listar todos os sites
        all_sites = list_all_plausible_sites(config, api_base)
        if not all_sites:
            print("❌ Nenhum site encontrado na conta Plausible")
            return 1
        
        # Buscar visitors para todos os sites
        visitors_df = get_visitors_for_all_sites(config, all_sites, yesterday, api_base)
        
        if visitors_df.empty:
            print("❌ Nenhum dado de visitors encontrado")
            return 1
        
        # Salvar resultados
        date_str = yesterday.replace("-", "")
        filename = f"plausible_visitors_{date_str}.csv"
        save_results(visitors_df, filename)
        
        # Mostrar resumo
        print(f"\n📊 RESUMO:")
        print(f"   - Sites analisados: {len(visitors_df)}")
        print(f"   - Total de visitors: {visitors_df['visitors'].sum():,.0f}")
        print(f"   - Média de visitors por site: {visitors_df['visitors'].mean():.1f}")
        
        # Top 10 sites por visitors
        top10 = visitors_df.nlargest(10, 'visitors')
        print(f"\n🏆 TOP 10 SITES POR VISITORS:")
        print("-" * 80)
        for idx, row in top10.iterrows():
            print(f"   {row['domain']:<40} {row['visitors']:>8,} visitors")
        
        # Estatísticas
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"   - Site com mais visitors: {visitors_df.loc[visitors_df['visitors'].idxmax(), 'domain']} ({visitors_df['visitors'].max():,})")
        print(f"   - Site com menos visitors: {visitors_df.loc[visitors_df['visitors'].idxmin(), 'domain']} ({visitors_df['visitors'].min():,})")
        print(f"   - Sites com 0 visitors: {len(visitors_df[visitors_df['visitors'] == 0])}")
        print(f"   - Sites com >1000 visitors: {len(visitors_df[visitors_df['visitors'] > 1000])}")
        
        # Distribuição por faixas
        print(f"\n📊 DISTRIBUIÇÃO POR FAIXAS DE VISITORS:")
        ranges = [
            (0, 0, "0 visitors"),
            (1, 100, "1 - 100"),
            (101, 1000, "101 - 1K"),
            (1001, 10000, "1K - 10K"),
            (10001, float('inf'), "10K+")
        ]
        
        for min_vis, max_vis, label in ranges:
            if min_vis == max_vis:
                count = len(visitors_df[visitors_df['visitors'] == min_vis])
            elif max_vis == float('inf'):
                count = len(visitors_df[visitors_df['visitors'] >= min_vis])
            else:
                count = len(visitors_df[(visitors_df['visitors'] >= min_vis) & (visitors_df['visitors'] <= max_vis)])
            print(f"   - {label:>12}: {count:>3} sites")
        
        print("\n🎉 Análise concluída!")
        print(f"\n💡 PRÓXIMO PASSO:")
        print("   Use este arquivo para cruzar com os dados de requests do ClickHouse")
        print(f"   Arquivo salvo: {filename}")
        
    except Exception as e:
        print(f"💥 ERRO: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
