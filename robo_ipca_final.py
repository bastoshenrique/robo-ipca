import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Função 1: Captura os dados do IPCA a partir do link utilizado
def fetch_ipca_data(url):
    """
    Faz a requisição HTTP para capturar os dados do IPCA no formato JSON.
    :param url: URL do endpoint do IBGE
    :return: Dados no formato JSON
    """
    response = requests.get(url)
    if response.status_code == 200:
        json_data = response.json()
        # print("JSON retornado pela API:", json_data)  # Comentei esta linha para ser usada apenas em depuração
        return json_data
    else:
        raise Exception(f"Erro ao acessar a URL: {response.status_code}")

# Função 2: Extrai todas as chaves e valores do JSON de forma dinâmica
def extract_all_keys(json_data, parent_key=''):
    """
    Extrai todas as chaves e valores de um JSON de forma recursiva.
    :param json_data: Dados no formato JSON (dicionário ou lista)
    :param parent_key: Chave pai para manter o contexto (usado em chamadas recursivas)
    :return: Lista de dicionários com todas as chaves e valores
    """
    items = []

    if isinstance(json_data, dict):  # Se for um dicionário
        for key, value in json_data.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            if isinstance(value, (dict, list)):  # Se o valor for outro dicionário ou lista, chama recursivamente
                items.extend(extract_all_keys(value, full_key))
            else:
                items.append({'chave': full_key, 'valor': value})

    elif isinstance(json_data, list):  # Se for uma lista
        for index, value in enumerate(json_data):
            full_key = f"{parent_key}[{index}]"
            if isinstance(value, (dict, list)):  # Se o valor for outro dicionário ou lista, chama recursivamente
                items.extend(extract_all_keys(value, full_key))
            else:
                items.append({'chave': full_key, 'valor': value})

    return items

# Função 3: Garante que todas as colunas do DataFrame tenham tipos consistentes
def clean_dataframe(df):
    """
    Garante que todas as colunas do DataFrame tenham tipos consistentes.
    :param df: DataFrame pandas
    :return: DataFrame pandas com tipos corrigidos
    """
    for column in df.columns:
        # Converte todos os valores para string
        df[column] = df[column].astype(str)
    return df

# Função 4: Salva o DataFrame em formato Parquet
def save_to_parquet(df, file_path):
    """
    Salva o DataFrame em um arquivo Parquet no disco local.
    :param df: DataFrame pandas
    :param file_path: Caminho do arquivo Parquet
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

# Função principal para executar o bot
def main():
    """
    Função principal que executa o bot por completo:
    1. Captura os dados do IPCA.
    2. Extrai todas as chaves e valores do JSON.
    3. Salva os dados em um arquivo Parquet.
    """
    # URL do IBGE
    url = "https://sidra.ibge.gov.br/ajax/json/tabela/1/1737?versao=-1"

    # Caminho do arquivo Parquet
    output_file = "ipca_data.parquet"

    # Etapas do processo
    print("Capturando os dados do IPCA...")
    json_data = fetch_ipca_data(url)

    print("Extraindo todas as chaves e valores do JSON...")
    extracted_data = extract_all_keys(json_data)

    print("Convertendo os dados para um DataFrame...")
    df = pd.DataFrame(extracted_data)

    print("Limpando os dados do DataFrame...")
    df = clean_dataframe(df)  # Garante que os tipos sejam consistentes

    print("Salvando os dados no formato Parquet...")
    save_to_parquet(df, output_file)

    print(f"Processo concluído! Arquivo salvo em: {output_file}")

# Executa o bot
if __name__ == "__main__":
    main()