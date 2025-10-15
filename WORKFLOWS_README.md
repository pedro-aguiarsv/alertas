# 🤖 Workflows GitHub Actions

Este projeto possui workflows automatizados para análise de sites e alertas.

## 📋 Workflows Disponíveis

### 1. 🚨 **Alerta Diário** (`alerta-diario.yml`)
- **Quando executa:** Diariamente às 9:00 AM (horário de Brasília)
- **O que faz:** 
  - Executa o script de monitoramento de sites com baixa receita
  - Envia alertas para Discord se encontrar sites problemáticos
  - Salva arquivo CSV como artefato

### 2. 🎛️ **Alerta Manual** (`alerta-manual.yml`)
- **Quando executa:** Sob demanda (execução manual)
- **O que faz:**
  - Permite executar o script a qualquer momento
  - Opção de modo de teste (não envia Discord)
  - Parâmetros customizáveis

### 3. 📊 **Análise Completa** (`analyze-all-sites.yml`)
- **Quando executa:** Diariamente às 10:00 AM (horário de Brasília)
- **O que faz:**
  - Analisa **TODOS** os sites do ClickHouse
  - Busca dados de visitors do Plausible
  - Cruza dados de requests vs visitors
  - Salva múltiplos arquivos CSV

### 4. 🔍 **Análise ClickHouse** (`clickhouse-only.yml`)
- **Quando executa:** Diariamente às 9:00 AM (horário de Brasília)
- **O que faz:**
  - Analisa **TODOS** os sites apenas do ClickHouse
  - Não depende do Plausible
  - Backup caso a integração Plausible falhe

## ⚙️ Configuração de Secrets

Configure os seguintes secrets no GitHub:

### **Obrigatórios (para todos os workflows):**
```env
CLICKHOUSE_URL=https://seu-servidor.clickhouse.com:8123
CLICKHOUSE_DATABASE=data_manifold
CLICKHOUSE_USER=seu_usuario
CLICKHOUSE_PASSWORD=sua_senha
```

### **Opcionais (para alertas Discord):**
```env
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
MENTION_IDS=<@123456789>
```

### **Opcionais (para integração Plausible):**
```env
PLAUSIBLE_API_TOKEN=seu_token_do_plausible
```

## 📁 Arquivos Gerados

### **Por data de execução:**
- `all_sites_requests_YYYYMMDD.csv` - Dados de requests de todos os sites
- `plausible_visitors_YYYYMMDD.csv` - Dados de visitors do Plausible
- `requests_vs_visitors_YYYYMMDD.csv` - Dados cruzados
- `sites_cost_pos_lowrev_yday_with_domain.csv` - Sites com baixa receita

### **Retenção:**
- Arquivos ficam disponíveis por **30 dias** no GitHub
- Baixe via seção "Artifacts" nas Actions

## 🚀 Como Usar

### **Execução Automática:**
Os workflows rodam automaticamente. Nenhuma ação necessária!

### **Execução Manual:**
1. Vá para **Actions** no GitHub
2. Selecione o workflow desejado
3. Clique em **"Run workflow"**
4. Configure parâmetros (se aplicável)
5. Clique em **"Run workflow"**

## 📊 Monitoramento

### **Ver logs:**
- Acesse **Actions** no GitHub
- Clique na execução desejada
- Veja logs detalhados de cada step

### **Download de arquivos:**
- Na página da execução, role até **"Artifacts"**
- Baixe o arquivo ZIP com todos os CSVs

### **Resumos:**
- Cada execução gera um resumo na seção **"Summary"**
- Mostra estatísticas e status de cada componente

## 🔧 Troubleshooting

### **Workflow falha:**
1. Verifique se todos os secrets estão configurados
2. Confira os logs para identificar o erro específico
3. Teste com execução manual primeiro

### **Plausible não funciona:**
- Use o workflow `clickhouse-only.yml` como backup
- Verifique se o token do Plausible está válido
- Confirme se tem permissões adequadas

### **Discord não recebe alertas:**
1. Verifique se o webhook URL está correto
2. Confirme se o bot tem permissões no canal
3. Teste o webhook manualmente

## 📈 Estatísticas Típicas

### **Dados de ontem (exemplo):**
- **181 sites** com requests
- **5,019,077 requests** no total
- **Média de 27,730 requests** por site
- **Top site:** noticiasdepernambuco.com.br (280,646 requests)

### **Distribuição:**
- **0-1K requests:** 58 sites
- **1K-10K requests:** 38 sites
- **10K-100K requests:** 74 sites
- **100K-1M requests:** 11 sites

## 🎯 Próximos Passos

- [ ] Configurar token do Plausible para integração completa
- [ ] Adicionar alertas para métricas específicas
- [ ] Criar dashboard de métricas
- [ ] Implementar notificações por email
- [ ] Adicionar análise de tendências

## 📚 Links Úteis

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Plausible API Documentation](https://plausible.io/docs/stats-api)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
