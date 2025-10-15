# 🚨 Sistema de Alertas - Sites com Baixa Receita

Este projeto monitora sites com custo positivo e baixa receita, enviando alertas automáticos via Discord.

## 🔧 Configuração

### 1. Secrets do GitHub

Configure os seguintes secrets no seu repositório GitHub:

1. Vá para **Settings** → **Secrets and variables** → **Actions**
2. Adicione os seguintes **Repository secrets**:

| Secret | Descrição | Exemplo |
|--------|-----------|---------|
| `CLICKHOUSE_URL` | URL do servidor ClickHouse | `https://seu-servidor.clickhouse.com:8123` |
| `CLICKHOUSE_DATABASE` | Nome da database | `sua_database` |
| `CLICKHOUSE_USER` | Usuário do ClickHouse | `seu_usuario` |
| `CLICKHOUSE_PASSWORD` | Senha do ClickHouse | `sua_senha` |
| `PLAUSIBLE_API_TOKEN` | Token da API do Plausible | `seu_token_do_plausible` |
| `DISCORD_WEBHOOK_URL` | URL do webhook do Discord | `https://discord.com/api/webhooks/...` |
| `MENTION_IDS` | IDs para mencionar no Discord | `<@123456789> <@987654321>` |

### 2. Como obter o Discord Webhook

1. No Discord, vá para **Configurações do Servidor** → **Integrações**
2. Clique em **Webhooks** → **Criar Webhook**
3. Configure o nome e canal
4. Copie a **URL do Webhook**

### 3. Como obter o Token do Plausible

1. Acesse [Plausible.io](https://plausible.io) e faça login
2. Vá para **Settings** → **API Keys**
3. Clique em **"Create API Key"**
4. Configure o nome e permissões (precisa de **read** para sites e stats)
5. Copie o **Token** gerado (começa com `eyJ`)

## 🤖 Workflows Disponíveis

### 1. 🚨 Alerta Diário (`alerta-diario.yml`)
- **Quando executa:** Automaticamente todos os dias às 9:00 AM (horário de Brasília)
- **O que faz:** 
  - Executa o script de monitoramento de sites com baixa receita
  - Envia alerta para Discord se encontrar sites problemáticos
  - Salva arquivo CSV como artefato

### 2. 🎛️ Alerta Manual (`alerta-manual.yml`)
- **Quando executa:** Sob demanda (execução manual)
- **O que faz:**
  - Permite executar o script a qualquer momento
  - Opção de modo de teste (não envia Discord)
  - Parâmetros customizáveis

### 3. 📊 Análise Completa (`analyze-all-sites.yml`)
- **Quando executa:** Diariamente às 10:00 AM (horário de Brasília)
- **O que faz:**
  - Analisa **TODOS** os sites do ClickHouse
  - Busca dados de visitors do Plausible
  - Cruza dados de requests vs visitors
  - Salva múltiplos arquivos CSV

### 4. 🔍 Análise ClickHouse (`clickhouse-only.yml`)
- **Quando executa:** Diariamente às 9:00 AM (horário de Brasília)
- **O que faz:**
  - Analisa **TODOS** os sites apenas do ClickHouse
  - Não depende do Plausible
  - Backup caso a integração Plausible falhe

> 📖 **Documentação completa:** Veja [WORKFLOWS_README.md](WORKFLOWS_README.md) para detalhes dos workflows

## 🚀 Como Usar

### Execução Automática
O script roda automaticamente todos os dias. Nenhuma ação necessária!

### Execução Manual
1. Vá para **Actions** no GitHub
2. Selecione **"Alerta Manual - Execução sob Demanda"**
3. Clique em **"Run workflow"**
4. Configure os parâmetros (opcional)
5. Clique em **"Run workflow"**

## 📁 Estrutura do Projeto

```
├── alerta_oficial.py          # Script principal
├── requirements.txt           # Dependências Python
├── .github/workflows/         # Workflows do GitHub Actions
│   ├── alerta-diario.yml     # Execução diária automática
│   └── alerta-manual.yml     # Execução manual
└── README.md                  # Esta documentação
```

## 🔍 Monitoramento

### Logs
- Acesse **Actions** no GitHub para ver os logs de execução
- Cada execução gera um resumo com estatísticas

### Artefatos
- Arquivos CSV são salvos como artefatos por 30 dias
- Baixe via interface do GitHub Actions

### Notificações
- Alertas são enviados via Discord quando sites problemáticos são encontrados
- Falhas no workflow podem ser configuradas para notificar

## 🛠️ Troubleshooting

### Script não executa
1. Verifique se todos os secrets estão configurados
2. Confira os logs no GitHub Actions
3. Teste com execução manual primeiro

### Não recebe alertas no Discord
1. Verifique se o webhook URL está correto
2. Confirme se o bot tem permissões no canal
3. Teste o webhook manualmente

### Problemas de conexão com ClickHouse
1. Verifique as credenciais nos secrets
2. Confirme se o servidor permite conexões externas
3. Teste a conectividade localmente primeiro

## 📊 Personalização

Para modificar os parâmetros do script, edite as constantes no início do arquivo `alerta_oficial.py`:

```python
MAX_REVENUE = 1.0        # Receita máxima para alerta
LOOKBACK_DAYS = 1        # Dias para busca de dados
FILTER_SITE_ID0 = True   # Filtrar site_id = 0
```

## 🔐 Segurança

- **Nunca** commite credenciais no código
- Use sempre GitHub Secrets para informações sensíveis
- Mantenha os webhooks do Discord privados
- Revise regularmente as permissões de acesso
