import requests
import xml.etree.ElementTree as ET
import urllib.parse
import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def investigar_arquivos():
    token_compartilhamento = "YggdBLfdninEJX9"
    url_webdav = "https://arquivos.receitafederal.gov.br/public.php/webdav/"

    print("Investigando o que há no servidor...")

    # Adicionando um User-Agent para evitar que o servidor rejeite a requisição
    headers = {
        'Depth': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    # O servidor fica atrás de um balanceador de carga (F5) que exige
    # cookies de sessão antes de aceitar PROPFIND — sem eles, retorna 200 vazio.
    # Tentativas de conexão em caso de instabilidade
    response = None
    for tentativa in range(3):
        try:
            session = requests.Session()
            session.verify = False
            session.get("https://arquivos.receitafederal.gov.br/", headers=headers, timeout=15)
            response = session.request("PROPFIND", url_webdav, auth=(token_compartilhamento, ''), headers=headers, timeout=15)
            response.raise_for_status()
            if response.content:
                break
            print(f"Tentativa {tentativa + 1}: resposta vazia do servidor, tentando novamente...")
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {tentativa + 1} falhou devido a erro de conexão: {e}")
        if tentativa < 2:
            time.sleep(3)
        else:
            print("Não foi possível estabelecer conexão com o servidor após 3 tentativas.")
            return

    root = ET.fromstring(response.content)
    namespaces = {'d': 'DAV:'}

    print("\n--- Conteúdo encontrado na raiz do link ---")

    # Lista tudo que o servidor retornou
    for response_node in root.findall('d:response', namespaces):
        href = response_node.find('d:href', namespaces).text
        nome_item = urllib.parse.unquote(href.strip('/').split('/')[-1])

        # O WebDAV retorna a própria raiz vazia no primeiro item, ignoramos
        if nome_item and nome_item != token_compartilhamento:
            print(f"Item: '{nome_item}'")

investigar_arquivos()