# PRG00 - Case Analytics: M&A em Logística

![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-orange)
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Data](https://img.shields.io/badge/Data%20Engineering-Polars%20%7C%20DuckDB-brightgreen)

## Descrição do Projeto

Este projeto aborda um caso prático analítico de **Fusões e Aquisições (M&A)** no setor de logística, focando na consolidação e exploração de dados operacionais das empresas **LogiPrime** e **RotaSul**. O objetivo central é realizar o diagnóstico detalhado da qualidade dos dados, estabelecer perfis operacionais de ambas as entidades e, principalmente, identificar estrategicamente **oportunidades de economia de Opex** e potenciais sinergias entre as operações logísticas.

A pipeline construída engloba a ingestão e conversão de dados brutos para o formato `.parquet`, tratamento qualificado de dados, e modelamento analítico para apurar métricas essenciais como o faturamento operacional médio por entrega.

---

## Autor e Identificação

**Nome:** Rubens Molina  
**Contato:** 
            E-mail: rubensloures43@gmail.com
            Linkedin: https://www.linkedin.com/in/rubens-molina/  
            GitHub: https://github.com/rubensloures
            Drive: https://drive.google.com/drive/folders/11Kdrt3Z6V5Ow7WuiS01vyo8VqUtNOU1v?usp=sharing - > Salvar os arquivos .csv na pasta data/01_raw/

---

## Tecnologias e Bibliotecas Utilizadas

A stack tecnológica do projeto foi definida com foco em desempenho analítico em memória para lidar com o robusto formato parquet gerado após a ingestão:

- **[Polars](https://pola.rs/):** Biblioteca para manipulação de DataFrames com altíssimo desempenho baseada em Rust e processamento multithread.
- **[DuckDB](https://duckdb.org/):** Motor SQL em processo e otimizado para consultas analíticas complexas direto na memória (OLAP).
- **[PyArrow](https://arrow.apache.org/docs/python/):** Facilita o backend em memória e manipulação para um layout de dados em colunas.

---

## Estrutura de Arquivos e Diretórios

O repositório está organizado dentro as melhores práticas de Arquitetura de Dados em lotes (Analytics):

```text
PRG00_Case M&A em Logistica/
├── data/
│   ├── 01_raw/        # Arquivos brutos originais enviados pelas empresas (CSVs)
│   ├── 02_interim/    # Artefatos e dados em processamento intermediário
│   ├── 03_processed/  # Dados limpos e preparados (formato consolidado .parquet)
│   ├── 04_models/     # Modelos (se aplicável)
│   └── 05_analysis/   # Relatórios quantitativos ou datasets gerados a partir do BI/análise
├── docs/              # Literatura do case e exports pdfs contendo documentação analítica
├── notebooks/         # Análises em profundidade (Diagnóstico de Qualidade, Oportunidades, Perfil)
├── src/               # Código-fonte principal base da pipeline ETL
│   ├── analysis/      # Arquivos de consolidação analítica e geração de métricas finais
│   ├── ingestion/     # Lógicas de importação dos dados raw e geração dos parquets
│   ├── modeling/      # Códigos de projeção/validação lógica
│   └── processing/    # Scripts aplicadores de regras de negócio e de limpeza estrutural
├── comandos.txt       # Guia curto dos comandos da pipeline CLI
├── main.py            # Orquestrador master (Ponto de entrada) da pipeline
├── README.md          # Documentação primária do repositório
└── requirements.txt   # Arquivo com dependências do ambiente virtual Python
```

---

## Instruções de Setup e Execução

Siga os passos a seguir para configurar sua máquina e replicar a conversão dos arquivos `raw`.

### 1. Preparação e Instalação de Dependências
Certifique-se de ativar seu ambiente virtual (venv) e instale os pacotes necessários via pip:
```bash
pip install -r requirements.txt
```

### 2. Conversão de Dados Brutos (Ingestion)
O primeiro passo no tratamento do projeto é a leitura e padronização. Para unificar os arquivos fragmentados da **RotaSul** em um único dataset analítico e, em seguida, transformá-los juntamente com os dados da **LogiPrime** para formato otimizado `.parquet`:
```bash
python src/ingestion/ingest.py
```

### 3. Aplicação das Regras de Negócio (Processing)
Processamento aprofundado a fim de solucionar valores nulos, inconsistências e modelar premissas do mercado (ex: conversão monetária, inserção do modelo real de rateio de margens e custos):
```bash
python src/processing/clean_data.py
```

---

## Jornada Analítica (Notebooks)

A pesquisa e auditoria da aquisição está subdividida lógicamente dentro do projeto em `notebooks/`:

- `01_Avaliacao_Qualidade.ipynb`
- `02_Perfil_Operacional.ipynb`
- `03_Oportunidade_Opex.ipynb`
- `04_Oportunidade_Entrega.ipynb`
- `05_Oportunidades_Melhoria.ipynb`
