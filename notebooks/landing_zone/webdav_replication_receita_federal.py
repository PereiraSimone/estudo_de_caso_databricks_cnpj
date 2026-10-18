# Databricks notebook source
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

# COMMAND ----------

import requests
import xml.etree.ElementTree as ET
import re
import urllib.parse
import urllib3
import time

# Desativa avisos de certificado (comum em sites governamentais)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def obter_links_receita_federal_nuvem():
    token_compartilhamento = "YggdBLfdninEJX9"
    url_webdav_base = "https://arquivos.receitafederal.gov.br/public.php/webdav/"

    headers = {
        'Depth': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    auth = (token_compartilhamento, '')

    print("1. Acessando o servidor para encontrar a pasta do mês mais recente...")

    session = requests.Session()
    session.verify = False
    session.get("https://arquivos.receitafederal.gov.br/", headers=headers, timeout=15)

    resp_raiz = None
    for tentativa in range(3):
        try:
            resp_raiz = session.request("PROPFIND", url_webdav_base, auth=auth, headers=headers, timeout=15)
            resp_raiz.raise_for_status()
            if resp_raiz.content:
                break
            print(f"Tentativa {tentativa + 1}: resposta vazia do servidor, tentando novamente...")
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {tentativa + 1} falhou: {e}")
        if tentativa < 2:
            time.sleep(3)

    if not resp_raiz or not resp_raiz.content:
        print("Não foi possível obter resposta do servidor após 3 tentativas.")
        return None

    root_raiz = ET.fromstring(resp_raiz.content)
    namespaces = {'d': 'DAV:'}

    pastas_meses = []

    # Procura pelas pastas no formato YYYY-MM
    for node in root_raiz.findall('d:response', namespaces):
        href = node.find('d:href', namespaces).text
        nome = urllib.parse.unquote(href.strip('/').split('/')[-1])

        # Se o nome for como "2026-08", guarda na lista
        if re.match(r'^\d{4}-\d{2}$', nome):
            pastas_meses.append(nome)

    if not pastas_meses:
        print("Nenhuma pasta de mês encontrada!")
        return None

    # Ordena as pastas e pega a última (a mais recente)
    pasta_recente = sorted(pastas_meses)[-1]
    print(f"-> Pasta mais atual encontrada: {pasta_recente}")

    print(f"\n2. Entrando na pasta '{pasta_recente}' para capturar os arquivos .zip...")

    # Agora fazemos a mesma requisição, mas entrando na pasta mais recente
    url_pasta = f"{url_webdav_base}{pasta_recente}/"
    resp_pasta = session.request("PROPFIND", url_pasta, auth=auth, headers=headers, timeout=15)
    resp_pasta.raise_for_status()
    root_pasta = ET.fromstring(resp_pasta.content)

    dicionario_links = {
        "Empresas": [],
        "Estabelecimentos": [],
        "Socios": [],
        "Tabelas_Apoio": []
    }

    # Para o link de download funcionar, precisamos colocar o nome da pasta no caminho
    path_download = urllib.parse.quote(f"/{pasta_recente}")
    url_download_base = f"https://arquivos.receitafederal.gov.br/index.php/s/{token_compartilhamento}/download?path={path_download}&files="

    for node in root_pasta.findall('d:response', namespaces):
        href = node.find('d:href', namespaces).text
        nome_arquivo = urllib.parse.unquote(href.strip('/').split('/')[-1])

        # Filtra apenas os arquivos que terminam com .zip (aceita .ZIP também)
        if nome_arquivo.lower().endswith('.zip'):
            link_completo = url_download_base + urllib.parse.quote(nome_arquivo)

            # Classifica o link baseado no nome
            if re.search(r'Empresas', nome_arquivo, re.IGNORECASE):
                dicionario_links["Empresas"].append(link_completo)
            elif re.search(r'Estabelecimentos', nome_arquivo, re.IGNORECASE):
                dicionario_links["Estabelecimentos"].append(link_completo)
            elif re.search(r'Socios', nome_arquivo, re.IGNORECASE):
                dicionario_links["Socios"].append(link_completo)
            else:
                dicionario_links["Tabelas_Apoio"].append(link_completo)

    return dicionario_links

# Testando a função
links_cnpj = obter_links_receita_federal_nuvem()

if links_cnpj:
    print("\n--- Links Capturados com Sucesso! ---")
    for categoria, lista_links in links_cnpj.items():
        print(f"\n[{categoria}] - {len(lista_links)} arquivo(s):")
        for link in lista_links[:3]: # Mostra os 3 primeiros para não poluir muito a tela
            print(f" - {link}")
        if len(lista_links) > 3:
            print(f" - ... e mais {len(lista_links) - 3} arquivo(s).")

# COMMAND ----------

import os
import re
import zipfile
import requests
import pandas as pd
import shutil
from collections import defaultdict
import xml.etree.ElementTree as ET
import urllib3
import urllib.parse
import time
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm.auto import tqdm
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# 1. FUNÇÃO PARA MAPEAMENTO DE TODOS OS MESES DE 2026 E TODAS AS CATEGORIAS
# =============================================================================
def mapear_links_2026():
    token_compartilhamento = "YggdBLfdninEJX9"
    url_webdav_base = "https://arquivos.receitafederal.gov.br/public.php/webdav/"
    headers = {'Depth': '1', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    auth = (token_compartilhamento, '')

    print("🔍 Mapeando diretórios de 2026 no servidor...")

    session = requests.Session()
    session.verify = False
    for tentativa_init in range(3):
        try:
            session.get("https://arquivos.receitafederal.gov.br/", headers=headers, timeout=30)
            break
        except requests.exceptions.RequestException:
            if tentativa_init < 2:
                time.sleep(3)

    resp_raiz = None
    for tentativa in range(3):
        try:
            resp_raiz = session.request("PROPFIND", url_webdav_base, auth=auth, headers=headers, timeout=15)
            resp_raiz.raise_for_status()
            if resp_raiz.content:
                break
            print(f"Tentativa {tentativa + 1}: resposta vazia do servidor, tentando novamente...")
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {tentativa + 1} falhou: {e}")
        if tentativa < 2:
            time.sleep(3)

    if not resp_raiz or not resp_raiz.content:
        print("❌ Não foi possível obter resposta do servidor após 3 tentativas.")
        return None

    root_raiz = ET.fromstring(resp_raiz.content)
    namespaces = {'d': 'DAV:'}
    
    pastas_2026 = []
    for node in root_raiz.findall('d:response', namespaces):
        href = node.find('d:href', namespaces).text
        nome = urllib.parse.unquote(href.strip('/').split('/')[-1])
        if re.match(r'^2026-\d{2}$', nome):
            pastas_2026.append(nome)

    if not pastas_2026:
        print("⚠️ Nenhuma pasta de 2026 encontrada!")
        return None

    print(f"✅ Encontrados {len(pastas_2026)} meses de 2026: {sorted(pastas_2026)}")

    dicionario_links = {
        "Empresas": [],
        "Estabelecimentos": [],
        "Socios": [],
        "Tabelas_Apoio": []
    }

    for pasta_mes in sorted(pastas_2026):
        print(f"   -> Extraindo links da pasta {pasta_mes}...")
        url_pasta = f"{url_webdav_base}{pasta_mes}/"
        
        resp_pasta = None
        for tentativa_pasta in range(3):
            try:
                resp_pasta = session.request("PROPFIND", url_pasta, auth=auth, headers=headers, timeout=15)
                resp_pasta.raise_for_status()
                if resp_pasta.content:
                    break
            except requests.exceptions.RequestException as e:
                print(f"   ⚠️ Tentativa {tentativa_pasta + 1} falhou: {e}")
            if tentativa_pasta < 2:
                time.sleep(3)

        if not resp_pasta or not resp_pasta.content:
            print(f"   ❌ Erro ao ler a pasta {pasta_mes}: resposta vazia após 3 tentativas.")
            continue

        root_pasta = ET.fromstring(resp_pasta.content)
            
        path_download = urllib.parse.quote(f"/{pasta_mes}")
        url_download_base = f"https://arquivos.receitafederal.gov.br/index.php/s/{token_compartilhamento}/download?path={path_download}&files="

        for node in root_pasta.findall('d:response', namespaces):
            href = node.find('d:href', namespaces).text
            nome_arquivo = urllib.parse.unquote(href.strip('/').split('/')[-1])

            if nome_arquivo.lower().endswith('.zip'):
                link_completo = url_download_base + urllib.parse.quote(nome_arquivo)

                if re.search(r'Empresas', nome_arquivo, re.IGNORECASE):
                    dicionario_links["Empresas"].append(link_completo)
                elif re.search(r'Estabelecimentos', nome_arquivo, re.IGNORECASE):
                    dicionario_links["Estabelecimentos"].append(link_completo)
                elif re.search(r'Socios', nome_arquivo, re.IGNORECASE):
                    dicionario_links["Socios"].append(link_completo)
                else:
                    dicionario_links["Tabelas_Apoio"].append(link_completo)

    return dicionario_links

# Executa o mapeamento
links_cnpj = mapear_links_2026()

# =============================================================================
# 2. CONFIGURAÇÕES DE INGESTÃO (DATABRICKS) E ESQUEMAS
# =============================================================================
pasta_base_volume = '/Volumes/workspace/default/cnpj_ingestion/bronze'
pasta_local = '/tmp/processamento_local'
os.makedirs(pasta_local, exist_ok=True)

esquemas_conhecidos = {
    "Empresas": [
        'cnpj_basico', 'razao_social', 'natureza_juridica',
        'qualificacao_responsavel', 'capital_social',
        'porte_empresa', 'ente_federativo_responsavel'
    ],
    "Estabelecimentos": [], 
    "Socios": [],
    "Tabelas_Apoio": []
}

_make_schema = pa.schema
schemas_pre = {cat: (_make_schema([(c, pa.string()) for c in cols]) if cols else None) for cat, cols in esquemas_conhecidos.items()}

def _schema_from_shape(n):
    return _make_schema([(f"col_{i}", pa.string()) for i in range(n)])

token_compartilhamento = "YggdBLfdninEJX9"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
session = requests.Session()
session.verify = False
for tentativa_init in range(3):
    try:
        session.get("https://arquivos.receitafederal.gov.br/", headers=headers, timeout=15)
        break
    except requests.exceptions.RequestException:
        if tentativa_init < 2:
            time.sleep(3)

# =============================================================================
# 3. FUNÇÃO DE PROCESSAMENTO PARALELO (DOWNLOAD -> EXTRACAO -> PARQUET)
# =============================================================================
def processar_arquivo_individual(args):
    index, url, mes, categoria, pasta_destino_drive, total_arquivos_mes = args
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    arquivo = qs['files'][0]

    # Arquivo de destino final. Se já existe, pula.
    arquivo_destino_final = f"{pasta_destino_drive}/{categoria.lower()}_{mes}_{index:02d}.parquet"
    if os.path.exists(arquivo_destino_final):
        return f"⏭️ Pulando: {arquivo} (Já existe no Volume)"

    url_direta_api = f"https://arquivos.receitafederal.gov.br/public.php/webdav/{mes}/{arquivo}"
    
    # Nomes únicos por thread para evitar condição de corrida
    nome_zip = f"{pasta_local}/temp_{categoria}_{mes}_{index}.zip"
    pasta_extracao = f"{pasta_local}/temp_csv_{categoria}_{mes}_{index}"
    arquivo_parquet_local = f"{pasta_local}/temp_pq_{categoria}_{mes}_{index}.parquet"
    
    os.makedirs(pasta_extracao, exist_ok=True)

    # 3.1: DOWNLOAD
    download_ok = False
    for tentativa_dl in range(3):
        try:
            resposta = session.get(url_direta_api, auth=(token_compartilhamento, ''), stream=True, verify=False, timeout=60, headers=headers)
            resposta.raise_for_status()
            
            with open(nome_zip, 'wb') as f:
                for chunk in resposta.iter_content(chunk_size=8*1024*1024):
                    if chunk:
                        f.write(chunk)
            
            if os.path.exists(nome_zip) and os.path.getsize(nome_zip) > 1000:
                download_ok = True
                break
        except Exception:
            if tentativa_dl < 2:
                time.sleep(3)

    if not download_ok:
        return f"❌ Falha ao baixar {arquivo}."

    # 3.2: EXTRAÇÃO
    with zipfile.ZipFile(nome_zip, 'r') as zip_ref:
        zip_ref.extractall(pasta_extracao)

    arquivo_csv_extraido = os.listdir(pasta_extracao)[0]
    caminho_csv = os.path.join(pasta_extracao, arquivo_csv_extraido)

    # 3.3: CONVERSÃO (PYARROW)
    colunas_categoria = esquemas_conhecidos.get(categoria, [])
    schema_pa = schemas_pre.get(categoria)
    
    df_args = {
        'filepath_or_buffer': caminho_csv,
        'sep': ';', 'encoding': 'latin1', 'dtype': str,
        'on_bad_lines': 'skip', 'chunksize': 500_000, 'header': None
    }
    if colunas_categoria:
        df_args['names'] = colunas_categoria

    df_iterator = pd.read_csv(**df_args)
    writer = None

    for df_chunk in df_iterator:
        if not colunas_categoria:
            df_chunk = df_chunk.set_axis([f"col_{i}" for i in range(df_chunk.shape[1])], axis=1)

        table = pa.Table.from_pandas(df_chunk, preserve_index=False)

        if writer is None:
            schema_final = schema_pa if schema_pa else _schema_from_shape(df_chunk.shape[1])
            writer = pq.ParquetWriter(arquivo_parquet_local, schema_final, compression='snappy')

        table = table.cast(writer.schema)
        writer.write_table(table)

    if writer:
        writer.close()

    # 3.4: MOVE PARA O VOLUME E LIMPA TEMPORÁRIOS
    shutil.copy(arquivo_parquet_local, arquivo_destino_final)
    
    os.remove(nome_zip)
    os.remove(caminho_csv)
    os.remove(arquivo_parquet_local)
    shutil.rmtree(pasta_extracao, ignore_errors=True)

    return f"✅ Salvo: {arquivo_destino_final.split('/')[-1]}"

# =============================================================================
# 4. LOOP MESTRE (PARALELIZADO)
# =============================================================================
tempo_inicio_total = time.time()

if links_cnpj:
    for categoria, links_categoria in links_cnpj.items():
        if not links_categoria:
            continue
            
        print(f"\n" + "="*60)
        print(f"🏢 INICIANDO INGESTÃO DA CATEGORIA: {categoria.upper()}")
        print("="*60)
        
        pasta_destino_drive = f"{pasta_base_volume}/{categoria.lower()}"
        os.makedirs(pasta_destino_drive, exist_ok=True)
        
        links_por_mes = defaultdict(list)
        for url in links_categoria:
            parsed = urllib.parse.urlparse(url)
            qs = urllib.parse.parse_qs(parsed.query)
            mes = qs['path'][0].strip('/')
            links_por_mes[mes].append(url)

        for mes in sorted(links_por_mes.keys()):
            links_do_mes = links_por_mes[mes]
            total_arquivos_mes = len(links_do_mes)
            print(f"\n🚀 {categoria} | MÊS: {mes} | Total de arquivos: {total_arquivos_mes}")

            # Prepara argumentos para o Pool de Threads
            tarefas = [
                (idx, url, mes, categoria, pasta_destino_drive, total_arquivos_mes)
                for idx, url in enumerate(links_do_mes, start=1)
            ]

            # Executa até 5 downloads/conversões simultâneas
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                for resultado in executor.map(processar_arquivo_individual, tarefas):
                    print(f"  {resultado}")

tempo_fim_total = time.time()
print("\n" + "="*60)
print(f"🎉 PIPELINE DE EXTRAÇÃO CONCLUÍDO!")
print(f"⏱️ Tempo total da operação: {(tempo_fim_total - tempo_inicio_total) / 60:.1f} minutos.")
print("="*60)


# COMMAND ----------


# =============================================================================
# 5. ATUALIZAÇÃO INCREMENTAL NO DATABRICKS (AUTO LOADER E MERGE)
# =============================================================================
# Este bloco deve ser executado no Databricks (PySpark) logo após a extração.
# Ele lê os novos arquivos do Volume de forma incremental e faz um UPSERT na tabela Silver/Gold.


from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

caminho_bronze = f"/Volumes/workspace/default/cnpj_ingestion/bronze/empresas/"
caminho_checkpoint = f"/Volumes/workspace/default/cnpj_ingestion/checkpoints/empresas_bronze_stream"
tabela_destino = "hive_metastore.seu_schema.empresas_silver"

# 1. Lê apenas arquivos novos via Auto Loader
df_incremental = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{caminho_checkpoint}_schema")
    .load(caminho_bronze)
)

# 2. Função de micro-batch para processar cada lote de novos arquivos e fazer o MERGE
def merge_micro_batch(df_batch, batch_id):
    if df_batch.isEmpty():
        return

    # Adiciona timestamp de controle (opcional)
    df_atualizado = df_batch.withColumn("data_ingestao", current_timestamp())

    # Se a tabela Delta já existir, faz MERGE (Upsert). Se não, cria a primeira vez.
    if spark.catalog.tableExists(tabela_destino):
        delta_table = DeltaTable.forName(spark, tabela_destino)
        
        (delta_table.alias("destino")
            .merge(
                df_atualizado.alias("origem"),
                "destino.cnpj_basico = origem.cnpj_basico" # Chave primária
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df_atualizado.write.format("delta").mode("overwrite").saveAsTable(tabela_destino)

# 3. Inicia o Stream incremental em modo Trigger Once (roda o que tem e desliga)
(df_incremental.writeStream
    .foreachBatch(merge_micro_batch)
    .option("checkpointLocation", caminho_checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
