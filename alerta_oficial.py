import os
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv
import requests  # NOVO: Importa a biblioteca para requisições HTTP

# ===== CONFIG =====
TZ = "America/Sao_Paulo"
DB_TABLE_REVENUE = "gam_impressions"
DB_TABLE_COST    = "gads_costs"
REVENUE_DIVISOR  = 1_000_000.0
COST_DIVISOR     = 1.0
MAX_REVENUE      = 1.0
LOOKBACK_DAYS    = 1
FILTER_SITE_ID0  = True
OUT_CSV = "sites_cost_pos_lowrev_yday_with_domain.csv"
# ==================

def get_db_config():
    """Carrega e retorna as configurações do banco e do webhook do .env."""
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
    
    config = {
        "url": os.getenv("CLICKHOUSE_URL"),
        "db":  os.getenv("CLICKHOUSE_DATABASE"),
        "usr": os.getenv("CLICKHOUSE_USER"),
        "pwd": os.getenv("CLICKHOUSE_PASSWORD"),
        "webhook_url": os.getenv("DISCORD_WEBHOOK_URL"),
        "mention_ids": os.getenv("MENTION_IDS")  # NOVO: Carrega os IDs para mencionar
    }
    
    if not all([config["url"], config["db"], config["usr"], config["pwd"]]):
        raise RuntimeError("Faltam variáveis do banco de dados no .env (CLICKHOUSE_URL/DATABASE/USER/PASSWORD)")
    
    return config


def get_client_db(config):
    """Cria e retorna um cliente de banco de dados com base na configuração."""
    p = urlparse(config["url"])
    client = clickhouse_connect.get_client(
        host=p.hostname, port=(p.port or 8123),
        username=config["usr"], password=config["pwd"], database=config["db"],
        secure=(p.scheme == "https"),
        settings={"readonly": 1},
    )
    return client

# NOVO: Função dedicada para enviar o alerta para o Discord
# MODIFICADO: A função agora aceita os IDs para menção
def send_discord_alert(webhook_url: str, df: pd.DataFrame, report_date: str, mention_ids: str | None):
    """
    Formata e envia um alerta para o Discord com um resumo dos sites encontrados.
    """
    if not webhook_url:
        print("AVISO: URL do webhook do Discord não configurada no .env. Alerta não enviado.")
        return

    num_sites = len(df)
    preview_data = df.head(10).to_string(index=False)

    embed = {
        "title": "🚨 Alerta: Sites com Custo e Baixa Receita",
        "color": 15158332, # Vermelho
        "description": f"Foram encontrados **{num_sites} sites** com `custo > 0` e `receita <= {MAX_REVENUE}` para a data de **{report_date}**.",
        "fields": [
            {"name": "Amostra dos Dados (até 10 sites):", "value": f"```\n{preview_data}\n```"},
            {"name": "Relatório Completo", "value": f"A lista completa foi salva no arquivo: `{OUT_CSV}`"}
        ]
    }

    # NOVO: O payload agora inclui o campo "content" para as menções
    payload = {
        "content": mention_ids if mention_ids else "", # Adiciona as menções aqui
        "username": "Monitor de Rentabilidade",
        "embeds": [embed]
    }

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("Alerta enviado para o Discord com sucesso!")
    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha ao enviar alerta para o Discord. Motivo: {e}")



def main():
    print("🚀 Iniciando execução do script de alertas...")
    
    try:
        config = get_db_config()
        print("✅ Configurações carregadas com sucesso")
        print(f"   - Database: {config['db']}")
        print(f"   - Webhook configurado: {'Sim' if config['webhook_url'] else 'Não'}")
    except Exception as e:
        print(f"❌ ERRO ao carregar configurações: {e}")
        raise
    
    try:
        client = get_client_db(config)
        print("✅ Conexão com banco estabelecida")
    except Exception as e:
        print(f"❌ ERRO ao conectar com banco: {e}")
        raise
        
    db = config["db"]
    
    today_sp = datetime.now(ZoneInfo(TZ)).date()
    yday_sp  = (today_sp - timedelta(days=1)).strftime("%Y-%m-%d")
    start_lb = (today_sp - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    
    print(f"📅 Datas calculadas:")
    print(f"   - Hoje (SP): {today_sp}")
    print(f"   - Ontem (SP): {yday_sp}")
    print(f"   - Lookback start: {start_lb}")

    # --- Revenue (GAM) — último timestamp DENTRO de ontem por site ---
    rev_latest_ts_sql = f"""
        SELECT
          site_id,
          max(toTimeZone(toDateTime(timestamp), '{TZ}')) AS ts_rev_latest_sp
        FROM {db}.{DB_TABLE_REVENUE}
        WHERE toDate(date) = toDate('{yday_sp}')
        GROUP BY site_id
    """
    rev_sum_sql = f"""
        SELECT
          r.site_id,
          argMax(r.domain, toTimeZone(toDateTime(r.timestamp), '{TZ}')) AS domain_yday,
          sum(r.ad_exchange_line_item_level_revenue) / {REVENUE_DIVISOR} AS revenue_latest
        FROM {db}.{DB_TABLE_REVENUE} AS r
        INNER JOIN ({rev_latest_ts_sql}) AS rl
          ON r.site_id = rl.site_id
         AND toTimeZone(toDateTime(r.timestamp), '{TZ}') = rl.ts_rev_latest_sp
        WHERE toDate(r.date) = toDate('{yday_sp}')
        GROUP BY r.site_id
    """

    # --- Domain fallback: últimos N dias no GAM (pega o mais recente na janela) ---
    domain_fallback_sql = f"""
        SELECT
          site_id,
          argMax(domain, toTimeZone(toDateTime(timestamp), '{TZ}')) AS domain_recent
        FROM {db}.{DB_TABLE_REVENUE}
        WHERE toDate(date) >= toDate('{start_lb}') AND toDate(date) <= toDate('{yday_sp}')
        GROUP BY site_id
    """

    # --- Custo (GADS) — último timestamp DENTRO de ontem por site ---
    cost_latest_ts_sql = f"""
        SELECT
          site_id,
          max(toTimeZone(toDateTime(timestamp), '{TZ}')) AS ts_cost_latest_sp
        FROM {db}.{DB_TABLE_COST}
        WHERE toDate(toTimeZone(toDateTime(timestamp), '{TZ}')) = toDate('{yday_sp}')
        GROUP BY site_id
    """
    cost_sum_sql = f"""
        SELECT
          c.site_id,
          sum(c.metrics_cost) / {COST_DIVISOR} AS cost_latest
        FROM {db}.{DB_TABLE_COST} AS c
        INNER JOIN ({cost_latest_ts_sql}) AS cl
          ON c.site_id = cl.site_id
         AND toTimeZone(toDateTime(c.timestamp), '{TZ}') = cl.ts_cost_latest_sp
        WHERE toDate(toTimeZone(toDateTime(c.timestamp), '{TZ}')) = toDate('{yday_sp}')
        GROUP BY c.site_id
    """

    # --- Junta revenue x cost; depois junta o domínio (ontem ou fallback 7d) ---
    final_sql = f"""
        WITH joined AS (
            SELECT
              coalesce(rs.site_id, cs.site_id) AS site_id,
              coalesce(rs.revenue_latest, 0)   AS revenue,
              coalesce(cs.cost_latest, 0)      AS cost,
              rs.domain_yday
            FROM ({rev_sum_sql}) AS rs
            FULL OUTER JOIN ({cost_sum_sql}) AS cs
              ON rs.site_id = cs.site_id
        ),
        with_domain AS (
            SELECT
              j.site_id,
              coalesce(j.domain_yday, df.domain_recent) AS domain,
              j.cost,
              j.revenue
            FROM joined j
            LEFT JOIN ({domain_fallback_sql}) AS df
              ON j.site_id = df.site_id
        )
        SELECT site_id, domain, cost, revenue
        FROM with_domain
        WHERE cost > 0
          AND revenue <= {MAX_REVENUE}
          {"AND site_id <> 0" if True else ""}
        ORDER BY site_id
    """

    print("🔍 Executando query principal...")
    try:
        df = client.query_df(final_sql) # Usei a sugestão de melhoria para carregar direto no DF
        print(f"✅ Query executada com sucesso. Linhas retornadas: {len(df)}")
    except Exception as e:
        print(f"❌ ERRO ao executar query: {e}")
        raise

    # Sempre criar o arquivo CSV, mesmo que vazio
    csv_path = Path(__file__).with_name(OUT_CSV)
    
    if not df.empty:
        print("📊 Processando dados encontrados...")
        if FILTER_SITE_ID0:
            df_before = len(df)
            df = df[df["site_id"] != 0].reset_index(drop=True)
            print(f"   - Filtrados {df_before - len(df)} sites com site_id=0")
        
        df["cost"] = df["cost"].astype(float).round(2)
        df["revenue"] = df["revenue"].astype(float).round(6)
        print(f"   - Dados processados: {len(df)} sites")

    print(f"📈 Sites com COST>0 e REVENUE<= {MAX_REVENUE} (ontem): {len(df)}")
    
    # SEMPRE salvar o CSV (mesmo que vazio para debugging)
    try:
        df.to_csv(csv_path, index=False)
        print(f"✅ Arquivo CSV salvo: {OUT_CSV} ({len(df)} linhas)")
    except Exception as e:
        print(f"❌ ERRO ao salvar CSV: {e}")
        # Criar um CSV vazio em caso de erro
        with open(csv_path, 'w') as f:
            f.write("site_id,domain,cost,revenue\n")
        print(f"⚠️  CSV vazio criado para debugging")
    
    if not df.empty:
        print("📋 Primeiros 15 resultados:")
        print(df.head(15))
        
        # MODIFICADO: A chamada da função agora passa os IDs de menção
        send_discord_alert(config["webhook_url"], df, yday_sp, config["mention_ids"])
    else:
        print("ℹ️  Nenhum site encontrado com os critérios definidos.")
        print("   - Isso pode ser normal se não há sites problemáticos")
        print("   - Arquivo CSV vazio foi criado para referência")
        print("   - Nenhum alerta enviado para Discord")


if __name__ == "__main__":
    try:
        main()
        print("🎉 Script executado com sucesso!")
    except Exception as e:
        print(f"💥 ERRO CRÍTICO: {e}")
        print("📋 Detalhes do erro:")
        import traceback
        traceback.print_exc()
        
        # Criar um arquivo CSV vazio para o workflow não falhar
        try:
            csv_path = Path(__file__).with_name(OUT_CSV)
            with open(csv_path, 'w') as f:
                f.write("site_id,domain,cost,revenue\n")
            print(f"⚠️  CSV vazio criado devido ao erro: {OUT_CSV}")
        except:
            print("❌ Não foi possível criar nem mesmo um CSV vazio")
        
        exit(1)