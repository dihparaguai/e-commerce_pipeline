# E-Commerce Data Pipeline (Medallion Architecture)

Este repositório contém um pipeline de engenharia de dados ponta a ponta projetado para simular um cenário real de e-commerce. O objetivo principal é ingerir dados diários de arquivos CSV, processá-los utilizando a arquitetura medalhão em um Data Lake (MinIO) e disponibilizar a camada analítica estruturada (fatos e dimensões) em um Data Warehouse (PostgreSQL) para consumo de inteligência de negócios.

---

## 1. Objetivo do Projeto
O projeto demonstra a implementação de um fluxo de dados robusto e orquestrado.
* **Cenário:** Uma plataforma de e-commerce gera arquivos diários em formato CSV (vendas, devoluções, estoques).
* **Solução:** Um pipeline automatizado que ingere esses arquivos brutos salvando-os na camada **Bronze** (Raw) do Data Lake, limpa e padroniza os dados salvando-os na camada **Silver** (Cleaned) no Data Lake, e finalmente carrega-os modelados no Data Warehouse (PostgreSQL) na camada **Gold** (Analytical).
* **Objetivo de Negócio:** Fornecer dados limpos e modelados em tabelas de Fato e Dimensões para responder a perguntas estratégicas de negócio por meio de um dashboard no Power BI.

---

## 2. Arquitetura de Dados (Camada Medalhão com Data Lake e Data Warehouse)
A arquitetura medalhão é híbrida, utilizando armazenamento em Object Storage e Banco de Dados Relacional:
1. **Camada `bronze` (Raw - Lake):** Armazenada no **MinIO** (Object Storage). Ingesta os dados convertendo os CSVs brutos em arquivos formato **Parquet**, estruturando os dados na sua forma original com histórico de carga.
2. **Camada `silver` (Cleaned - Lake):** Armazenada no **MinIO** (Object Storage) também em formato **Parquet**. Contém dados limpos e padronizados prontos para modelagem.
3. **Camada `gold` (Analytical - DW):** Armazenada no **PostgreSQL** (Data Warehouse). Estruturada em tabelas dimensionais (Fatos e Dimensões usando Star Schema) otimizadas para consultas rápidas e consumo direto pelo Power BI.

---

## 3. Tecnologias Utilizadas
* **Linguagem Principal:** [Python 3.12](https://docs.python.org/3.12/)
* **Processamento de Dados:** [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) (processamento massivo) e [Pandas](https://pandas.pydata.org/docs/) (análise exploratória rápida)
* **Banco de Dados / Data Warehouse:** [PostgreSQL](https://www.postgresql.org/docs/)
* **Object Storage (Lakehouse):** [MinIO](https://min.io/docs/minio/linux/index.html) (simulação de S3/Cloud Storage)
* **Orquestração:** [Apache Airflow](https://airflow.apache.org/docs/) (para agendamento e gerenciamento do pipeline)
* **Conteinerização:** [Docker](https://docs.docker.com/) e [Docker Compose](https://docs.docker.com/compose/) (isolamento do ambiente e dependências)
* **ORM e Conexão:** [SQLAlchemy](https://docs.sqlalchemy.org/en/20/)
* **Monitoramento e Logs:** [Loguru](https://loguru.readthedocs.io/)
* **Qualidade e Testes:** [Pytest](https://docs.pytest.org/)
* **Visualização:** [Power BI](https://learn.microsoft.com/power-bi/)

---

## 4. Origem dos Scripts e Imagens Docker
As imagens de infraestrutura utilizadas neste projeto foram obtidas diretamente de fontes oficiais:
* **Apache Airflow (Script Docker Compose Pronto para Uso):** O arquivo base `docker-compose.yaml` foi obtido a partir da documentação oficial do [Apache Airflow Docker Compose Quick Start](https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml).
* **Apache Spark:** Imagem Docker oficial [apache/spark:3.5.1](https://hub.docker.com/r/apache/spark) baixada do Docker Hub.
* **MinIO:** Imagem Docker oficial [quay.io/minio/minio](https://quay.io/repository/minio/minio) baixada do Quay.io.

---

## 5. Estrutura do Projeto e Pastas
```text
e-commerce_pipeline/
├── dags/                 # DAGs do Apache Airflow para orquestração
├── data/                 # Arquivos CSV diários recebidos
├── docker/               # Dockerfiles utilizados no projeto
├── docs/                 # Diagramas e documentação do projeto
├── notebooks/            # Jupyter Notebooks para análise exploratória e testes
└── src/                  # Código-fonte do pipeline
│   ├── main.py           # Ponto de entrada do pipeline
│   ├── config/           # Configurações globais e de banco de dados
│   ├── sql/              # Scripts SQL para criação das tabelas
│   ├── jobs/             # Jobs dos pipelines (bronze, silver e gold)
│   └── modules/          # Módulos reutilizáveis (classes, transformações, loads)
├── tests/                # Testes do pipeline
├── .env                  # Variáveis de ambiente (credenciais, paths)
├── .gitignore            # Arquivos ignorados pelo Git
├── docker-compose.yml    # Configuração do Docker Compose
├── README.md             # Documentação do projeto
└── requirements.txt      # Dependências de bibliotecas Python
```

---

## 6. Configurações e Dependências

### Pré-requisitos
* Git
* Docker e Docker Compose instalado
* Python 3.12 (caso queira executar scripts isoladamente local)

### Variáveis de Ambiente
Crie um arquivo `.env` na raiz do projeto com base no modelo abaixo:
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=ecommerce_dw
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

---

## 7. Como Executar

### a. Clonar o Repositório
```bash
git clone https://github.com/seu-usuario/e-commerce_pipeline.git
cd e-commerce_pipeline
```

### b. Iniciar a Infraestrutura (Docker)
Suba os containers do PostgreSQL e do Apache Airflow:
```bash
docker-compose up -d
```

### c. Execução Local do Pipeline (Sem Airflow)
Caso queira testar o pipeline Python localmente de forma direta:
```bash
# Criar ambiente virtual
python -m venv .venv
source .venv/bin/activate  # No Windows: .venv\Scripts\activate

# Instalar dependências
pip install -r requirements.txt

# Executar o fluxo principal
python src/main.py
```

---

## 8. Melhorias Futuras
* **Data Lake na Nuvem:** Substituir o armazenamento local por Azure Data Lake Storage Gen 2.
* **CI/CD Pipeline:** Configurar GitHub Actions para rodar testes automatizados (`pytest`).
