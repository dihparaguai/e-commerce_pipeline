# E-Commerce Data Pipeline (Medallion Architecture)

Este repositório contém um pipeline de engenharia de dados ponta a ponta projetado para simular um cenário real de e-commerce. O objetivo principal é ingerir dados diários de arquivos CSV, processá-los utilizando a arquitetura medalhão e disponibilizar tabelas estruturadas (fatos e dimensões) em um banco de dados relacional para consumo de inteligência de negócios.

---

## 1. Objetivo do Projeto
O projeto demonstra a implementação de um fluxo de dados robusto e orquestrado.
* **Cenário:** Uma plataforma de e-commerce gera arquivos diários em formato CSV (vendas, devoluções, estoques).
* **Solução:** Um pipeline automatizado que ingere esses arquivos brutos, aplica regras de negócios, trata inconsistências e popula um Data Warehouse (PostgreSQL) estruturado em camadas (Bronze, Silver, Gold).
* **Objetivo de Negócio:** Fornecer dados limpos e modelados em tabelas de Fato e Dimensões para responder a perguntas estratégicas de negócio por meio de um dashboard no Power BI.

---

## 2. Arquitetura de Dados (Camada Medalhão no PostgreSQL)
Toda a arquitetura medalhão reside no banco de dados **PostgreSQL**, estruturada através de schemas dedicados:
1. **Schema `bronze` (Raw Layer):** Tabelas que recebem os dados brutos dos CSVs exatamente como foram ingeridos, mantendo a estrutura original e histórico.
2. **Schema `silver` (Cleaned Layer):** Tabelas com dados limpos, padronizados (tipos de dados corretos, nulos tratados, remoção de duplicatas) e prontos para transformação.
3. **Schema `gold` (Analytical Layer):** Tabelas modeladas em formato dimensional (Fatos e Dimensões usando Star Schema) otimizadas para consultas analíticas e consumo no Power BI.

---

## 3. Tecnologias Utilizadas
* **Linguagem Principal:** Python 3.11+
* **Processamento de Dados:** PySpark (processamento massivo) e Pandas (análise exploratória rápida)
* **Banco de Dados / Data Warehouse:** PostgreSQL
* **Orquestração:** Apache Airflow (para agendamento e gerenciamento do pipeline)
* **Conteinerização:** Docker e Docker Compose (isolamento do ambiente e dependências)
* **ORM e Conexão:** SQLAlchemy
* **Monitoramento e Logs:** Loguru
* **Qualidade e Testes:** Pytest
* **Visualização:** Power BI

---

## 4. Estrutura do Projeto e Pastas
```text
e-commerce_pipeline/
├── data/                 # Arquivos CSV diários recebidos
├── dags/                 # DAGs do Apache Airflow para orquestração
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

## 5. Configurações e Dependências

### Pré-requisitos
* Git
* Docker e Docker Compose instalado
* Python 3.11+ (caso queira executar scripts isoladamente local)

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

## 6. Como Executar

### 1. Clonar o Repositório
```bash
git clone https://github.com/seu-usuario/e-commerce_pipeline.git
cd e-commerce_pipeline
```

### 2. Iniciar a Infraestrutura (Docker)
Suba os containers do PostgreSQL e do Apache Airflow:
```bash
docker-compose up -d
```

### 3. Execução Local do Pipeline (Sem Airflow)
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

## 7. Melhorias Futuras
* **Data Lake na Nuvem:** Substituir o armazenamento local por Azure Data Lake Storage Gen 2.
* **CI/CD Pipeline:** Configurar GitHub Actions para rodar testes automatizados (`pytest`).
