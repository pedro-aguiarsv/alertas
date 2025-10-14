#!/bin/bash

echo "🔍 Explorador de Banco de Dados - Alertas"
echo "========================================"

# Verificar se o .env existe
if [ ! -f ".env" ]; then
    echo "❌ Arquivo .env não encontrado!"
    echo "📝 Crie um arquivo .env com as seguintes variáveis:"
    echo "   CLICKHOUSE_URL=sua_url"
    echo "   CLICKHOUSE_DATABASE=sua_database"
    echo "   CLICKHOUSE_USER=seu_usuario"
    echo "   CLICKHOUSE_PASSWORD=sua_senha"
    exit 1
fi

# Instalar dependências se necessário
echo "📦 Verificando dependências..."
pip install -r requirements_explore.txt

# Executar o script
echo "🚀 Executando explorador..."
python explore_database.py

echo "✅ Concluído!"
