# 🔍 Explorador de Banco de Dados

Este script permite explorar a estrutura das tabelas do seu banco ClickHouse de forma simples e direta.

## 📋 O que ele faz

- **Lista todas as tabelas** disponíveis no banco
- **Mostra a estrutura completa** das tabelas `gam_impressions` e `gads_costs`
- **Exibe amostras dos dados** reais de cada tabela
- **Conta o total de linhas** em cada tabela

## 🚀 Como usar

### Opção 1: Script automatizado (recomendado)
```bash
./run_explore.sh
```

### Opção 2: Manual
```bash
# Instalar dependências
pip install -r requirements_explore.txt

# Executar o explorador
python explore_database.py
```

## ⚙️ Configuração

Certifique-se de que o arquivo `.env` contém:

```env
CLICKHOUSE_URL=https://seu-servidor.clickhouse.com:8123
CLICKHOUSE_DATABASE=sua_database
CLICKHOUSE_USER=seu_usuario
CLICKHOUSE_PASSWORD=sua_senha
```

## 📊 Exemplo de saída

```
🚀 EXPLORADOR DE BANCO DE DADOS
============================================================
✅ Conectando ao banco: sua_database
✅ Conexão estabelecida!

============================================================
📊 INFORMAÇÕES GERAIS DO BANCO: sua_database
============================================================

🗂️  TABELAS DISPONÍVEIS (2 encontradas):
--------------------------------------------------
  - gam_impressions
  - gads_costs

============================================================
📋 ESTRUTURA DA TABELA: gam_impressions
============================================================

🔍 COLUNAS (8 encontradas):
--------------------------------------------------------------------------------
Nome                           Tipo                       Comentário            
--------------------------------------------------------------------------------
site_id                        UInt64                                          
domain                         String                                          
timestamp                      DateTime                                        
date                           Date                                            
ad_exchange_line_item_level_revenue Float64                                    
[outras colunas...]

📊 Total de linhas: 1,234,567

📋 AMOSTRA DOS DADOS (5 linhas):
----------------------------------------------------------------------------------------------------
Colunas: site_id | domain | timestamp | date | ad_exchange_line_item_level_revenue
----------------------------------------------------------------------------------------------------
Linha 1: 123 | example.com | 2024-01-15 10:30:00 | 2024-01-15 | 0.000123
Linha 2: 456 | test.com | 2024-01-15 11:45:00 | 2024-01-15 | 0.000456
...
```

## 🔧 Personalização

Para analisar outras tabelas, edite o arquivo `explore_database.py`:

```python
# Linha 89: Adicione o nome da tabela que deseja analisar
tables_of_interest = ['gam_impressions', 'gads_costs', 'sua_nova_tabela']
```

## 🐛 Troubleshooting

- **Erro de conexão:** Verifique as credenciais no `.env`
- **Tabela não encontrada:** Confirme o nome exato da tabela
- **Permissões:** Certifique-se de que o usuário tem acesso de leitura

## 📁 Arquivos

- `explore_database.py` - Script principal
- `requirements_explore.txt` - Dependências
- `run_explore.sh` - Script de execução automatizada
- `EXPLORE_README.md` - Esta documentação
