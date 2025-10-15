#!/usr/bin/env python3
"""
Script simples para testar o token do Plausible
"""

import os
import requests
from pathlib import Path
from dotenv import load_dotenv

def test_token():
    """Testa o token do Plausible."""
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
    
    token = os.getenv("PLAUSIBLE_API_TOKEN")
    
    if not token:
        print("❌ Token não encontrado no .env")
        print("📝 Adicione: PLAUSIBLE_API_TOKEN=seu_token_aqui")
        return
    
    print(f"🔍 Token encontrado: {token[:20]}...")
    
    # Testar diferentes APIs
    api_urls = [
        "https://wearenalytics.com/api/v1",
        "https://wearenalytics.com/api/v2", 
        "https://plausible.io/api/v1",
        "https://plausible.io/api/v2"
    ]
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    for api_url in api_urls:
        try:
            print(f"🧪 Testando: {api_url}/sites")
            response = requests.get(
                f"{api_url}/sites",
                headers=headers,
                timeout=10
            )
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ SUCESSO! {len(data)} sites encontrados")
                return api_url
            elif response.status_code == 401:
                print(f"   ❌ Token inválido/expirado")
            elif response.status_code == 403:
                print(f"   ❌ Token sem permissões")
            elif response.status_code == 404:
                print(f"   ❌ URL não encontrada")
            else:
                print(f"   ⚠️  Status inesperado: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Erro: {e}")
    
    print("\n❌ Nenhuma API funcionou")
    print("\n💡 VERIFICAÇÕES:")
    print("1. Token está correto no .env?")
    print("2. Token tem permissões adequadas?")
    print("3. Token não expirou?")
    print("4. Usando a conta correta?")

if __name__ == "__main__":
    test_token()
