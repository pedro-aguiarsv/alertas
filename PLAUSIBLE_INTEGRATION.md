# 📊 Integração ClickHouse + Plausible

Este script cruza dados de `ad_exchange_total_requests` do ClickHouse com dados de visitors do Plausible para análise de performance.

## 🎯 Objetivo

- Buscar dados de **requests** da tabela `gam_ecpms` (ClickHouse)
- Buscar dados de **visitors** da API do Plausible
- Cruzar os dados por domínio e data
- Calcular métricas como requests/visitor e visitors/request

## ⚙️ Configuração

### 1. Variáveis de Ambiente

Adicione ao seu arquivo `.env`:

```env
# ClickHouse (já existentes)
CLICKHOUSE_URL=https://seu-servidor.clickhouse.com:8123
CLICKHOUSE_DATABASE=sua_database
CLICKHOUSE_USER=seu_usuario
CLICKHOUSE_PASSWORD=sua_senha

# Plausible (NOVAS)
PLAUSIBLE_API_TOKEN=seu_token_do_plausible
PLAUSIBLE_SITE_ID=seu_site_id_do_plausible
```

### 2. Como obter credenciais do Plausible

#### **API Token:**
1. Acesse [Plausible.io](https://plausible.io)
2. Vá em **Settings** → **API Keys**
3. Clique em **Create API Key**
4. Copie o token gerado

#### **Site ID:**
1. No Plausible, vá para seu site
2. Na URL você verá algo como: `https://plausible.io/site/example.com`
3. O Site ID é `example.com` (sem o protocolo)

## 🚀 Como usar

### Execução simples:
```bash
python3 plausible_integration.py
```

### Instalar dependências (se necessário):
```bash
pip install requests pandas clickhouse-connect python-dotenv
```

## 📊 O que o script faz

### 1. **Busca dados do ClickHouse:**
```sql
SELECT 
    site_id,
    date,
    domain,
    SUM(ad_exchange_total_requests) as total_requests
FROM gam_ecpms
WHERE date >= 'data_inicio' AND date <= 'data_fim'
GROUP BY site_id, date, domain
```

### 2. **Busca dados do Plausible:**
- Usa a API `/stats/breakdown` para obter visitors por página
- Agrupa por domínio
- Filtra pelo período especificado

### 3. **Cruza os dados:**
- Merge por domínio e data
- Calcula métricas:
  - `requests_per_visitor` = requests / visitors
  - `visitors_per_request` = visitors / requests

### 4. **Salva resultados:**
- Arquivo CSV com timestamp: `requests_vs_visitors_YYYYMMDD_HHMMSS.csv`

## 📋 Estrutura do CSV de saída

| Coluna | Descrição |
|--------|-----------|
| `site_id` | ID do site (ClickHouse) |
| `date` | Data |
| `domain` | Domínio do site |
| `total_requests` | Total de requests (ClickHouse) |
| `visitors` | Total de visitors (Plausible) |
| `requests_per_visitor` | Requests por visitor |
| `visitors_per_request` | Visitors por request |

## 🔧 Personalização

### Alterar período de análise:
```python
LOOKBACK_DAYS = 30  # Analisar últimos 30 dias
```

### Filtrar por site específico:
```python
# No método get_plausible_visitors(), adicione:
params["filters"] = f"event:page=={site_domain}"
```

### Adicionar mais métricas:
```python
# No método get_plausible_visitors():
params["metrics"] = "visitors,pageviews,bounce_rate"
```

## 📊 Exemplo de saída

```
🚀 INTEGRAÇÃO CLICKHOUSE + PLAUSIBLE
============================================================
✅ Configurações carregadas
✅ Conectado ao ClickHouse
📅 Período: 2024-01-15 a 2024-01-22
🔍 Buscando dados de requests de 2024-01-15 a 2024-01-22...
✅ Encontrados 1,234 registros de requests
📊 Buscando visitors para todos os domínios...
✅ Encontrados visitors para 45 domínios
🔄 Cruzando dados de requests com visitors...
✅ Cruzamento concluído: 1,156 registros
💾 Dados salvos em: requests_vs_visitors_20240122_143052.csv

📊 RESUMO:
   - Sites analisados: 25
   - Domínios únicos: 45
   - Total de requests: 2,345,678
   - Total de visitors: 89,123

🏆 TOP 10 SITES POR REQUESTS:
   - Site 123: 456,789 requests
   - Site 456: 234,567 requests
   ...
```

## 🐛 Troubleshooting

### Erro de autenticação Plausible:
- Verifique se o token está correto
- Confirme se o Site ID está no formato correto

### Sem dados de requests:
- Verifique se a tabela `gam_ecpms` tem dados no período
- Confirme se a coluna `ad_exchange_total_requests` existe

### Sem dados de visitors:
- Verifique se o site está configurado no Plausible
- Confirme se há dados no período especificado

## 📚 API Reference

- [Plausible API Documentation](https://plausible.io/docs/stats-api)
- [ClickHouse Documentation](https://clickhouse.com/docs/)

## 🔄 Próximos passos

- [ ] Adicionar alertas automáticos
- [ ] Criar dashboard de métricas
- [ ] Integrar com GitHub Actions
- [ ] Adicionar mais fontes de dados
