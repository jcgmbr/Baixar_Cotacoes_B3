# =========================
# IMPORTS
# =========================
import pandas as pd
import os
import requests
import zipfile
from io import BytesIO

# =========================
# BANCO DE DADOS
# =========================
from sqlalchemy import text
from conexao.conexao_db import ConexaoBD


# =========================
# DOWNLOAD DOS ARQUIVOS (100% ROBUSTO)
# =========================
def download_b3_files(years, download_path):
    base_url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/"
    os.makedirs(download_path, exist_ok=True)

    for year in years:
        zip_name = f"COTAHIST_A{year}.ZIP"
        url = base_url + zip_name

        print(f"\n📥 Baixando {zip_name}...")

        try:
            response = requests.get(url)
            response.raise_for_status()

            with zipfile.ZipFile(BytesIO(response.content)) as z:

                members = z.namelist()

                if not members:
                    print(f"⚠️ ZIP vazio: {zip_name}")
                    continue

                # 🔥 pega o primeiro arquivo (padrão da B3)
                member = members[0]

                target_name = f"COTAHIST_A{year}.TXT"
                target_path = os.path.join(download_path, target_name)

                # Remove arquivo antigo
                if os.path.exists(target_path):
                    os.remove(target_path)

                # Extrai e renomeia corretamente
                with z.open(member) as source, open(target_path, "wb") as target:
                    target.write(source.read())

                print(f"✅ Arquivo salvo como: {target_name}")

        except Exception as e:
            print(f"❌ Erro ao baixar {zip_name}: {e}")


# =========================
# LEITURA DOS ARQUIVOS
# =========================
def read_files(path, year):
    file_path = os.path.join(path, f"COTAHIST_A{year}.TXT")

    if not os.path.exists(file_path):
        print(f"⚠️ Arquivo não encontrado: {file_path}")
        return None

    colspecs = [
        (2, 10), (10, 12), (12, 24), (27, 39),
        (56, 69), (69, 82), (82, 95), (108, 121),
        (152, 170), (170, 188)
    ]

    names = [
        'data_pregao', 'codbdi', 'ticker', 'nome_acao',
        'abertura', 'maximo', 'minimo', 'fechamento',
        'qtd_negocios', 'volume'
    ]

    try:
        df = pd.read_fwf(
            file_path,
            colspecs=colspecs,
            names=names,
            skiprows=1,
            encoding='latin-1'
        )
        return df

    except Exception as e:
        print(f"❌ Erro ao ler {file_path}: {e}")
        return None


# =========================
# TRANSFORMAÇÃO
# =========================
def transform(df):
    df = df[df['codbdi'] == 2].copy()
    df.drop(columns=['codbdi'], inplace=True)

    df['data_pregao'] = pd.to_datetime(
        df['data_pregao'],
        format='%Y%m%d',
        errors='coerce'
    )

    cols = ['abertura', 'maximo', 'minimo', 'fechamento']
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce') / 100

    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['qtd_negocios'] = pd.to_numeric(df['qtd_negocios'], errors='coerce')

    df.dropna(inplace=True)

    return df


# =========================
# BANCO DE DADOS
# =========================
def criar_schema(engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS cotacoes"))
        conn.commit()
        print("✅ Schema 'cotacoes' pronto")


def carregar_csv_para_banco(caminho_csv):
    try:
        conexao = ConexaoBD()
        engine = conexao.conectar()

        criar_schema(engine)

        print("📂 Lendo CSV...")
        df = pd.read_csv(caminho_csv)

        print(f"📊 Total de linhas: {len(df):,}")

        print("🚀 Enviando dados para o banco...")

        df.to_sql(
            name="cotacao_acoes",
            con=engine,
            schema="cotacoes",
            if_exists="replace",   # use "append" se quiser acumular
            index=False,
            chunksize=10000,
            method="multi"
        )

        print("✅ Dados inseridos com sucesso!")

    except Exception as e:
        print("❌ Erro na carga:")
        print(e)

# =========================
# LIMPEZA DOS ARQUIVOS BRUTOS (APENAS B3)
# =========================
def limpar_dados_brutos(path):
    try:
        arquivos = os.listdir(path)

        for arquivo in arquivos:
            # 🔥 filtra apenas arquivos padrão da B3
            if arquivo.startswith("COTAHIST_A") and arquivo.endswith(".TXT"):

                file_path = os.path.join(path, arquivo)

                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"🗑️ Removido: {arquivo}")

        print("🧹 Limpeza seletiva concluída!")

    except Exception as e:
        print(f"❌ Erro ao limpar dados brutos: {e}")


# =========================
# PIPELINE PRINCIPAL
# =========================
def run_pipeline(years, raw_path, final_file):

    print("\n🚀 INICIANDO PIPELINE...\n")

    # 1. Download
    download_b3_files(years, raw_path)

    # 2. Processamento
    dfs = []

    for year in years:
        print(f"\n📊 Processando {year}...")

        df = read_files(raw_path, year)

        if df is None:
            continue

        df = transform(df)
        dfs.append(df)

    if not dfs:
        print("❌ Nenhum dado processado.")
        return

    df_final = pd.concat(dfs, ignore_index=True)

    # 3. Salvar + enviar ao banco
    try:
        df_final.to_csv(final_file, index=False, encoding='utf-8')
        print(f"\n✅ Arquivo salvo em:\n{final_file}")

        # 🔥 envio correto ao banco
        carregar_csv_para_banco(final_file)

        # 🧹 limpeza dos arquivos brutos
        limpar_dados_brutos(raw_path)

    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")


# =========================
# EXECUÇÃO
# =========================
if __name__ == "__main__":

    years = list(range(2000, 2027))

    # Diretório base do script
    base_dir = os.path.dirname(os.path.abspath(__file__))

    raw_path = os.path.join(base_dir, "dados_brutos")
    output_dir = os.path.join(base_dir, "cotacoes_acoes")

    os.makedirs(raw_path, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    final_file = os.path.join(output_dir, "all_bovespa.csv")

    run_pipeline(years, raw_path, final_file)
    