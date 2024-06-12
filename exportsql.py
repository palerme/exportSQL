import pandas as pd
import pyodbc
import os

# Configurações de conexão
server = 'NBGUILHERME\\SQL2014'
database = 'Gerencial_KinaDelPaseo'
username = 'sa'
password = 'sisco12!@'

# String de conexão
conn_str = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    'TrustServerCertificate=yes'
)

# Conectar ao banco de dados
conn = pyodbc.connect(conn_str)

# Definir diretório base onde as pastas serão salvas
base_output_dir = r'C:\Users\joaog\OneDrive\Documentos\Migração'

# Definir diretório específico para o banco de dados
output_dir = os.path.join(base_output_dir, 'Migração  ' + database)

# Certifique-se de que o diretório existe
os.makedirs(output_dir, exist_ok=True)

# Função para executar query e salvar como CSV
def export_to_csv(query, filename):
    df = pd.read_sql_query(query, conn)
    output_path = os.path.join(output_dir, filename)
    df.to_csv(output_path, sep=';', index=False)

# Suas queries
queries = {
    "categorias.csv": """
        SELECT
            Código AS codigo,
            Descrição AS descricao,
            DataDaInclusão AS datadainclusao,
            DataDeEdição AS datadeedicao,
            Status AS status,
            Regras AS regras,
            AutoAtendimento AS autoatendimento,
            Visivel AS visivel,
            Prioridade AS prioridade,
            Handheld AS handheld,
            Favorito AS favorito
        FROM
            Categorias c;
    """,
    "produtos.csv": """
        SELECT
            Código AS codigo,
            Descrição AS descricao,
            DataDaInclusão AS datadainclusao,
            DataDeEdição AS datadeedicao,
            Ativo AS ativo,
            Tipo AS tipo,
            Status AS status,
            Fabricação AS fabricacao,
            Arredonda AS arredonda,
            UNA AS una,
            Categoria AS categoria,
            Disponível AS disponivel,
            ProdutoEncerrandoAtendimento AS produtoencerrandoatendimento,
            ProdutoGTIN AS produtogtin,
            ProdutoNCM AS produtoncm,
            UNAFator AS unafator,
            UNE AS une,
            Setor AS setor,
            Fracionado AS fracionado,
            Apelido AS apelido,
            Peso AS peso
        FROM
            Produtos p;
    """,
    "produtos_composicoes.csv": """
        SELECT
            Código AS codigo,
            Produto AS produtoid,
            Produto2 AS produto2id,
            Status AS status,
            Ativo AS ativo,
            QDE AS qde,
            Fator AS fator,
            Tipo AS tipo,
            Baixar AS baixar,
            PreçoZero AS precozero,
            QdeFixa AS qdefixa
        FROM
            Produtos_Composições pc;
    """,
    "precos.csv": """
        SELECT
            PrecoID AS precoid,
            PrecoNome AS preconome,
            Inclusao AS inclusao,
            Edicao AS edicao,
            Status AS status,
            Regras AS regras
        FROM
            Precos p;
    """,
    "produtos_precos_de_venda.csv": """
        SELECT
            ProdutoID AS produtoid,
            PrecoID AS precoid,
            Edicao AS edicao,
            Status AS status,
            Preco AS preco
        FROM
            ProdutosPrecosDeVenda ppdv;
    """,
    "fornecedores.csv": """
        SELECT
            Código AS codigo,
            DataDaInclusão AS datadainclusao,
            DataDeEdição AS datadeedicao,
            Status AS status,
            Ativo AS ativo,
            Usuário AS usuario,
            Nome AS nome,
            Apelido AS apelido,
            Unidade AS unidade,
            Funcionário AS funcionario,
            Vendedor AS vendedor,
            Garçon AS garcon,
            Entregador AS entregador,
            Comissão AS comissao,
            Comissão1 AS comissao1,
            Comissão2 AS comissao2,
            Comissão3 AS comissao3,
            Comissão4 AS comissao4,
            Comissão5 AS comissao5,
            CEP AS cep,
            Endereço AS endereco,
            Número AS numero,
            Complemento AS complemento,
            Bairro AS bairro,
            Município AS municipio,
            Cidade AS cidade,
            UF AS uf,
            Referência AS referencia,
            Telefones AS telefones,
            CNPJ AS cnpj,
            IE AS ie,
            RazãoSocial AS razaosocial,
            RG AS rg,
            CPF AS cpf
        FROM
            Fornecedores f
        WHERE
            Vendedor = 1
            OR Garçon = 1
            OR Entregador = 1;
    """
}

# Executando queries e salvando resultados em CSV
for filename, query in queries.items():
    export_to_csv(query, filename)

# Fechando a conexão com o banco de dados
conn.close()